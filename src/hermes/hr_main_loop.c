//
// Created by vasilis on 10/09/20.
//

#include "hr_inline_util.h"

static inline void fill_inv(hr_inv_t *inv,
                            ctx_trace_op_t *op,
                            hr_w_rob_t *w_rob)
{
  if (ENABLE_ASSERTIONS) assert(op->opcode == KVS_OP_PUT);
  memcpy(&inv->key, &op->key, KEY_SIZE);
  memcpy(inv->value, op->value_to_write, (size_t) VALUE_SIZE);
  inv->version = w_rob->version;

}



static inline void hr_batch_from_trace_to_KVS(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = hr_ctx->ops;
  trace_t *trace = hr_ctx->trace;

  uint16_t op_i = 0;
  int working_session = -1;

  if (all_sessions_are_stalled(ctx, hr_ctx->all_sessions_stalled,
                               &hr_ctx->stalled_sessions_dbg_counter))
    return;
  if (!od_find_starting_session(ctx, hr_ctx->last_session,
                                hr_ctx->stalled, &working_session)) return;

  bool passed_over_all_sessions = false;

  /// main loop
  while (op_i < HR_TRACE_BATCH && !passed_over_all_sessions) {

    od_fill_trace_op(ctx, &trace[hr_ctx->trace_iter], &ops[op_i], working_session);
    hr_ctx->stalled[working_session] = true;

    passed_over_all_sessions =
        od_find_next_working_session(ctx, &working_session,
                                     hr_ctx->stalled,
                                     hr_ctx->last_session,
                                     &hr_ctx->all_sessions_stalled);
    if (!ENABLE_CLIENTS) {
      hr_ctx->trace_iter++;
      if (trace[hr_ctx->trace_iter].opcode == NOP) hr_ctx->trace_iter = 0;
    }
    op_i++;
  }
  hr_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].total_reqs += op_i;
  hr_KVS_batch_op_trace(ctx, op_i);
  if (!INSERT_WRITES_FROM_KVS) {
    for (int i = 0; i < hr_ctx->ptrs_to_inv->polled_invs; ++i) {
      od_insert_mes(ctx, INV_QP_ID, (uint32_t) INV_SIZE, 1,
                    false, hr_ctx->ptrs_to_inv->ptr_to_ops[i], 0, 0);
    }
  }
}



///* ---------------------------------------------------------------------------
////------------------------------ COMMIT WRITES -----------------------------
////---------------------------------------------------------------------------*/

static inline void apply_writes(context_t *ctx,
                                hr_w_rob_t **ptrs_to_w_rob,
                                uint16_t write_num)
{
  for (int w_i = 0; w_i < write_num; ++w_i) {
    hr_w_rob_t *w_rob = ptrs_to_w_rob[w_i];
    if (w_rob->inv_applied) {
      if (ENABLE_ASSERTIONS) {
        assert(w_rob->version > 0);
        assert(w_rob != NULL);
      }
      mica_op_t *kv_ptr = w_rob->kv_ptr;
      lock_seqlock(&kv_ptr->seqlock);
      {
        //assert(kv_ptr->m_id == w_rob->m_id);
        if (ENABLE_ASSERTIONS) assert(kv_ptr->version > 0);
        if (kv_ptr->version == w_rob->version &&
            kv_ptr->m_id == w_rob->m_id) {
          if (ENABLE_ASSERTIONS) {
            if (w_rob->m_id == ctx->m_id)
              assert(kv_ptr->state == HR_W);
            else
              assert(kv_ptr->state == HR_INV ||
                     kv_ptr->state == HR_INV_T);
          }
          kv_ptr->state = HR_V;
        }

      }
      unlock_seqlock(&kv_ptr->seqlock);
    }
  }
}

static inline void complete_local_write(context_t * ctx,
                                        hr_w_rob_t *w_rob)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  uint16_t sess_id = w_rob->sess_id;
  if (ENABLE_ASSERTIONS) {
    assert(w_rob->acks_seen == REM_MACH_NUM);
    assert(sess_id < SESSIONS_PER_THREAD);
    assert(hr_ctx->stalled[sess_id]);
  }
  w_rob->acks_seen = 0;
  signal_completion_to_client(sess_id,
                              hr_ctx->index_to_req_array[sess_id],
                              ctx->t_id);
  hr_ctx->stalled[sess_id] = false;
}

static inline void hr_commit_writes(context_t *ctx)
{
  uint16_t write_num = 0, local_op_i = 0;
  hr_w_rob_t *ptrs_to_w_rob[HR_UPDATE_BATCH];
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  for (int m_i = 0; m_i < MACHINE_NUM; ++m_i) {
    hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_pull_slot(&hr_ctx->w_rob[m_i]);
    while (w_rob->w_state == READY) {
      __builtin_prefetch(&w_rob->kv_ptr->seqlock, 0, 0);
      w_rob->w_state = INVALID;
      if (ENABLE_ASSERTIONS)
        assert(write_num < HR_UPDATE_BATCH);
      ptrs_to_w_rob[write_num] = w_rob;
      //my_printf(green, "Commit sess %u write %lu, version: %lu \n",
      //          w_rob->sess_id, hr_ctx->committed_w_id[m_i] + write_num, w_rob->version);

      if (m_i == ctx->m_id) {
        complete_local_write(ctx, w_rob);
        local_op_i++;
      }
      fifo_incr_pull_ptr(&hr_ctx->w_rob[m_i]);
      fifo_decrem_capacity(&hr_ctx->w_rob[m_i]);
      w_rob = (hr_w_rob_t *) get_fifo_pull_slot(&hr_ctx->w_rob[m_i]);
      write_num++;
    }
  }

  if (write_num > 0) {
    apply_writes(ctx, ptrs_to_w_rob, write_num);
    if (local_op_i > 0) {
      hr_ctx->all_sessions_stalled = false;
      ctx_insert_commit(ctx, COM_QP_ID, local_op_i, hr_ctx->committed_w_id[ctx->m_id]);
      hr_ctx->committed_w_id[ctx->m_id] += local_op_i;
    }
  }
}

///* ---------------------------------------------------------------------------
////------------------------------SEND HELPERS -----------------------------
////---------------------------------------------------------------------------*/

//Inserts a LOCAL inv to the buffer
inline void insert_inv_help(context_t *ctx, void* inv_ptr,
                            void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[INV_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;

  hr_inv_t *inv = (hr_inv_t *) inv_ptr;
  ctx_trace_op_t *op = (ctx_trace_op_t *) source;
  hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_push_slot(hr_ctx->loc_w_rob);

  check_w_rob_in_insert_help(ctx, op, w_rob);
  fill_inv(inv, op, w_rob);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  hr_inv_mes_t *inv_mes = (hr_inv_mes_t *) get_fifo_push_slot(send_fifo);
  inv_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    inv_mes->l_id = hr_ctx->inserted_w_id[ctx->m_id];
    fifo_set_push_backward_ptr(send_fifo, hr_ctx->loc_w_rob->push_ptr);
  }
  // Bookkeeping
  w_rob->w_state = VALID;
  fifo_incr_push_ptr(hr_ctx->loc_w_rob);
  hr_ctx->inserted_w_id[ctx->m_id]++;
  hr_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
}


inline void send_acks_helper(context_t *ctx)
{
  ctx_refill_recvs(ctx, COM_QP_ID);
}

inline void send_invs_helper(context_t *ctx)
{
  hr_checks_and_stats_on_bcasting_invs(ctx);
}


inline void hr_send_commits_helper(context_t *ctx)
{
  hr_checks_and_stats_on_bcasting_commits(ctx);
}


///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/
inline bool inv_handler(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[INV_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile hr_inv_mes_ud_t *incoming_invs = (volatile hr_inv_mes_ud_t *) recv_fifo->fifo;
  hr_inv_mes_t *inv_mes = (hr_inv_mes_t *) &incoming_invs[recv_fifo->pull_ptr].inv_mes;

  uint8_t coalesce_num = inv_mes->coalesce_num;

  fifo_t *w_rob_fifo = &hr_ctx->w_rob[inv_mes->m_id];
  bool invs_fit_in_w_rob =
    w_rob_fifo->capacity + coalesce_num <= w_rob_fifo->max_size;

  if (!invs_fit_in_w_rob) return false;

  fifo_increase_capacity(w_rob_fifo, coalesce_num);

  hr_check_polled_inv_and_print(ctx, inv_mes);
  ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  inv_mes->l_id, inv_mes->m_id);

  ptrs_to_inv_t *ptrs_to_inv = hr_ctx->ptrs_to_inv;
  if (qp_meta->polled_messages == 0) ptrs_to_inv->polled_invs = 0;

  for (uint8_t inv_i = 0; inv_i < coalesce_num; inv_i++) {
    check_w_rob_when_handling_an_inv(ptrs_to_inv, w_rob_fifo, inv_mes, inv_i);
    ptrs_to_inv->ptr_to_ops[ptrs_to_inv->polled_invs] = &inv_mes->inv[inv_i];
    ptrs_to_inv->ptr_to_mes[ptrs_to_inv->polled_invs] = inv_mes;
    ptrs_to_inv->polled_invs++;
  }
  if (ENABLE_ASSERTIONS) inv_mes->opcode = 0;
  return true;
}



static inline void hr_apply_acks(context_t *ctx,
                                 ctx_ack_mes_t *ack,
                                 uint32_t ack_num, uint32_t ack_ptr)
{

  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
    hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_slot(hr_ctx->loc_w_rob, ack_ptr);
    w_rob->acks_seen++;
    check_when_applying_acks(ctx, w_rob, ack, ack_num, ack_ptr, ack_i);
    if (w_rob->acks_seen == REM_MACH_NUM)
      w_rob->w_state = READY;
    MOD_INCR(ack_ptr, HR_PENDING_WRITES);
  }
}


inline bool ack_handler(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;
  uint32_t ack_num = ack->ack_num;
  uint64_t l_id = ack->l_id;
  uint64_t pull_lid = hr_ctx->committed_w_id[ctx->m_id]; // l_id at the pull pointer
  uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  per_qp_meta_t *com_qp_meta = &ctx->qp_meta[COM_QP_ID];
  com_qp_meta->credits[ack->m_id] = com_qp_meta->max_credits;

  if ((hr_ctx->loc_w_rob->capacity == 0 ) ||
      (pull_lid >= l_id && (pull_lid - l_id) >= ack_num))
    return true;

  ack_ptr = ctx_find_when_the_ack_points_acked(ack, hr_ctx->loc_w_rob, pull_lid, &ack_num);

  // Apply the acks that refer to stored writes
  hr_apply_acks(ctx, ack, ack_num, ack_ptr);
  return true;
}


inline bool hr_commit_handler(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_com_mes_ud_t *incoming_coms = (volatile ctx_com_mes_ud_t *) recv_fifo->fifo;

  ctx_com_mes_t *com = (ctx_com_mes_t *) &incoming_coms[recv_fifo->pull_ptr].com;
  uint32_t com_num = com->com_num;
  uint64_t l_id = com->l_id;
  hr_check_polled_commit_and_print(ctx, com, recv_fifo->pull_ptr);

  fifo_t *w_rob_fifo = &hr_ctx->w_rob[com->m_id];
  /// loop through each commit
  for (uint16_t com_i = 0; com_i < com_num; com_i++) {
    hr_w_rob_t * w_rob = get_fifo_slot_mod(w_rob_fifo, (uint32_t) (l_id + com_i));
    hr_check_each_commit(ctx, com, w_rob, com_i);
    hr_ctx->committed_w_id[com->m_id]++;
    w_rob->w_state = READY;
  } ///

  if (ENABLE_ASSERTIONS) com->opcode = 0;
  return true;
}


_Noreturn inline void hr_main_loop(context_t *ctx)
{
  if (ctx->t_id == 0) my_printf(yellow, "Hermes main loop \n");
  while(true) {
    hr_batch_from_trace_to_KVS(ctx);
    ctx_send_broadcasts(ctx, INV_QP_ID);
    ctx_poll_incoming_messages(ctx, INV_QP_ID);
    od_send_acks(ctx, ACK_QP_ID);
    ctx_poll_incoming_messages(ctx, ACK_QP_ID);
    ctx_send_broadcasts(ctx, COM_QP_ID);
    ctx_poll_incoming_messages(ctx, COM_QP_ID);
    hr_commit_writes(ctx);
  }
}
