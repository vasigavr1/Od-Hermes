//
// Created by vasilis on 07/09/20.
//

#ifndef ODYSSEY_HR_UTIL_H
#define ODYSSEY_HR_UTIL_H


#include "hr_config.h"
#include "network_context.h"
#include <hr_inline_util.h>
#include <trace_util.h>
#include "init_func.h"

void hr_stats(stats_ctx_t *ctx);

static void hr_static_assert_compile_parameters()
{

  emphatic_print(green, "Hermes");
}

static void hr_init_functionality(int argc, char *argv[])
{
  generic_static_assert_compile_parameters();
  hr_static_assert_compile_parameters();
  generic_init_globals(QP_NUM);
  //hr_init_globals();
  handle_program_inputs(argc, argv);
}


static void hr_init_send_fifos(context_t *ctx)
{
  fifo_t *send_fifo = ctx->qp_meta[VAL_QP_ID].send_fifo;
  ctx_com_mes_t *vals = (ctx_com_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < VAL_FIFO_SIZE; i++) {
    vals[i].opcode = KVS_OP_PUT;
    vals[i].m_id = ctx->m_id;
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
    ack_send_buf[i].m_id = (uint8_t) machine_id;
    ack_send_buf[i].opcode = OP_ACK;
  }


}

static void hr_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  //mfs[PREP_QP_ID].recv_handler = prepare_handler;
  //mfs[PREP_QP_ID].send_helper = send_prepares_helper;
  //mfs[PREP_QP_ID].insert_helper = insert_prep_help;
  //mfs[PREP_QP_ID].polling_debug = hr_debug_info_bookkeep;

  //mfs[ACK_QP_ID].recv_handler = ack_handler;

  //mfs[COM_QP_ID].recv_handler = hr_commit_handler;
  //mfs[COM_QP_ID].send_helper = hr_send_commits_helper;
  //mfs[COMMIT_W_QP_ID].polling_debug = hr_debug_info_bookkeep;
  //
  //mfs[R_QP_ID].recv_handler = r_handler;
  //mfs[ACK_QP_ID].send_helper = send_acks_helper;
  //mfs[R_QP_ID].recv_kvs = hr_KVS_batch_op_reads;
  //mfs[R_QP_ID].insert_helper = insert_r_rep_help;
  //mfs[R_QP_ID].polling_debug = hr_debug_info_bookkeep;



  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}

static void hr_init_qp_meta(context_t *ctx)
{
  per_qp_meta_t *qp_meta = ctx->qp_meta;
  create_per_qp_meta(&qp_meta[VAL_QP_ID], MAX_VAL_WRS,
                     MAX_RECV_VAL_WRS, SEND_BCAST_RECV_BCAST,  RECV_REQ,
                     ACK_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, VAL_BUF_SLOTS,
                     VAL_RECV_SIZE, VAL_SEND_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     VAL_MCAST_QP, 0, VAL_FIFO_SIZE,
                     VAL_CREDITS, VAL_MES_HEADER,
                     "send val", "recv val");

  crate_ack_qp_meta(&qp_meta[ACK_QP_ID],
                    VAL_QP_ID, REM_MACH_NUM,
                    REM_MACH_NUM, VAL_CREDITS);

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


static void* set_up_hr_ctx(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) calloc(1, sizeof(hr_ctx_t));


  hr_ctx->w_rob = fifo_constructor(HR_PENDING_WRITES, sizeof(w_rob_t), false, 0, 1);

  hr_ctx->index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));

  hr_ctx->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));

  hr_ctx->ops = (hr_trace_op_t *) calloc((size_t) HR_TRACE_BATCH, sizeof(hr_trace_op_t));
  hr_ctx->resp = (hr_resp_t*) calloc((size_t) HR_TRACE_BATCH, sizeof(hr_resp_t));
  for(int i = 0; i <  HR_TRACE_BATCH; i++) hr_ctx->resp[i].type = EMPTY;

  for (int i = 0; i < SESSIONS_PER_THREAD; i++) hr_ctx->stalled[i] = false;
  for (uint32_t i = 0; i < HR_PENDING_WRITES; i++) {
    w_rob_t *w_rob = get_fifo_slot(hr_ctx->w_rob, i);
    w_rob->w_state = INVALID;
    w_rob->version = 0;
    w_rob->w_rob_id = (uint16_t) i;
  }

  if (!ENABLE_CLIENTS)
    hr_ctx->trace = trace_init(ctx->t_id);
}



typedef struct stats {
  double batch_size_per_thread[WORKERS_PER_MACHINE];
  double com_batch_size[WORKERS_PER_MACHINE];
  double prep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
  double stalled_gid[WORKERS_PER_MACHINE];
  double stalled_ack_prep[WORKERS_PER_MACHINE];
  double stalled_com_credit[WORKERS_PER_MACHINE];


  double cache_hits_per_thread[WORKERS_PER_MACHINE];


  double preps_sent[WORKERS_PER_MACHINE];
  double acks_sent[WORKERS_PER_MACHINE];
  double coms_sent[WORKERS_PER_MACHINE];

  double received_coms[WORKERS_PER_MACHINE];
  double received_acks[WORKERS_PER_MACHINE];
  double received_preps[WORKERS_PER_MACHINE];

  double write_ratio_per_client[WORKERS_PER_MACHINE];
} all_stats_t;
#endif //ODYSSEY_HR_UTIL_H