//
// Created by vasilis on 10/09/20.
//

#include "hr_util.h"

void hr_static_assert_compile_parameters()
{

  emphatic_print(green, "Hermes");
}

void hr_init_functionality(int argc, char *argv[])
{
  od_generic_static_assert_compile_parameters();
  hr_static_assert_compile_parameters();
  od_generic_init_globals(QP_NUM);
  //hr_init_globals();
  od_handle_program_inputs(argc, argv);
}


void hr_init_send_fifos(context_t *ctx)
{
  fifo_t *send_fifo = ctx->qp_meta[INV_QP_ID].send_fifo;
  hr_inv_mes_t *invs = (hr_inv_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < INV_FIFO_SIZE; i++) {
    invs[i].opcode = KVS_OP_PUT;
    invs[i].m_id = ctx->m_id;
  }


  send_fifo = ctx->qp_meta[COM_QP_ID].send_fifo;
  ctx_com_mes_t *commits = (ctx_com_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < COMMIT_FIFO_SIZE; i++) {
    commits[i].opcode = COMMIT_OP;
    commits[i].m_id = ctx->m_id;
  }

  ctx_ack_mes_t *ack_send_buf = (ctx_ack_mes_t *) ctx->qp_meta[ACK_QP_ID].send_fifo->fifo; //calloc(MACHINE_NUM, sizeof(ctx_ack_mes_t));
  assert(ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size == CTX_ACK_SIZE * MACHINE_NUM);
  memset(ack_send_buf, 0, ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size);
  for (int i = 0; i < MACHINE_NUM; i++) {
    ack_send_buf[i].m_id = ctx->m_id;
    ack_send_buf[i].opcode = OP_ACK;
  }


}

void hr_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  mfs[INV_QP_ID].recv_handler = inv_handler;
  mfs[INV_QP_ID].send_helper = send_invs_helper;
  mfs[INV_QP_ID].insert_helper = insert_inv_help;
  mfs[INV_QP_ID].recv_kvs = hr_KVS_batch_op_invs;
  //mfs[PREP_QP_ID].polling_debug = hr_debug_info_bookkeep;

  mfs[ACK_QP_ID].recv_handler = ack_handler;
  mfs[ACK_QP_ID].send_helper = send_acks_helper;

  mfs[COM_QP_ID].recv_handler = hr_commit_handler;
  mfs[COM_QP_ID].send_helper = hr_send_commits_helper;
  //mfs[COMMIT_W_QP_ID].polling_debug = hr_debug_info_bookkeep;
  //
  //mfs[R_QP_ID].recv_handler = r_handler;
  //

  //mfs[R_QP_ID].insert_helper = insert_r_rep_help;
  //mfs[R_QP_ID].polling_debug = hr_debug_info_bookkeep;



  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}

void hr_init_qp_meta(context_t *ctx)
{
  per_qp_meta_t *qp_meta = ctx->qp_meta;
  create_per_qp_meta(&qp_meta[INV_QP_ID], MAX_INV_WRS,
                     MAX_RECV_INV_WRS, SEND_BCAST_RECV_BCAST, RECV_REQ,
                     ACK_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, INV_BUF_SLOTS,
                     INV_RECV_SIZE, INV_SEND_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     VAL_MCAST_QP, 0, INV_FIFO_SIZE,
                     INV_CREDITS, INV_MES_HEADER,
                     "send val", "recv val");

  create_ack_qp_meta(&qp_meta[ACK_QP_ID],
                     INV_QP_ID, REM_MACH_NUM,
                     REM_MACH_NUM, INV_CREDITS);

  create_per_qp_meta(&qp_meta[COM_QP_ID], COM_WRS,
                     RECV_COM_WRS, SEND_BCAST_RECV_BCAST, RECV_SEC_ROUND,
                     COM_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, COM_BUF_SLOTS,
                     CTX_COM_RECV_SIZE, CTX_COM_SEND_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     COM_MCAST_QP, 0, COMMIT_FIFO_SIZE,
                     COM_CREDITS, CTX_COM_SEND_SIZE,
                     "send commits", "recv commits");


  hr_qp_meta_mfs(ctx);
  hr_init_send_fifos(ctx);
}


void* set_up_hr_ctx(context_t *ctx)
{

  hr_ctx_t *hr_ctx = (hr_ctx_t *) calloc(1, sizeof(hr_ctx_t));

  hr_ctx->w_rob = fifo_constructor(HR_PENDING_WRITES, sizeof(hr_w_rob_t), false, 0, MACHINE_NUM);
  hr_ctx->loc_w_rob = &hr_ctx->w_rob[ctx->m_id];
  hr_ctx->index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));

  hr_ctx->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));

  hr_ctx->committed_w_id = calloc(MACHINE_NUM, sizeof(uint64_t));
  hr_ctx->inserted_w_id = calloc(MACHINE_NUM, sizeof(uint64_t));

  hr_ctx->ops = (ctx_trace_op_t *) calloc((size_t) HR_TRACE_BATCH, sizeof(ctx_trace_op_t));

  hr_ctx->buf_ops = fifo_constructor(2 * SESSIONS_PER_THREAD, sizeof(buf_op_t), false, 0, 1);


  for (int i = 0; i < SESSIONS_PER_THREAD; i++) hr_ctx->stalled[i] = false;

  for (uint8_t m_id = 0; m_id < MACHINE_NUM; ++m_id) {
    for (uint32_t i = 0; i < HR_PENDING_WRITES; i++) {
      hr_w_rob_t *w_rob = get_fifo_slot(&hr_ctx->w_rob[m_id], i);
      w_rob->w_state = INVALID;
      w_rob->version = 0;
      w_rob->m_id = m_id;
      w_rob->id = (uint16_t) i;
      if (m_id == ctx->m_id) {
        w_rob->inv_applied = true;
      }
    }
  }

  hr_ctx->ptrs_to_inv = calloc(1, sizeof(ptrs_to_inv_t));
  hr_ctx->ptrs_to_inv->ptr_to_ops = calloc(MAX_INCOMING_INV, sizeof(hr_inv_t*));
  hr_ctx->ptrs_to_inv->ptr_to_mes = calloc(MAX_INCOMING_INV, sizeof(hr_inv_mes_t*));


  if (!ENABLE_CLIENTS)
    hr_ctx->trace = trace_init(ctx->t_id);
  return hr_ctx;
}