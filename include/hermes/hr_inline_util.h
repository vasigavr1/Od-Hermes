//
// Created by vasilis on 07/09/20.
//

#ifndef ODYSSEY_HR_INLINE_UTIL_H
#define ODYSSEY_HR_INLINE_UTIL_H


#include <inline_util.h>
#include "network_context.h"
#include "hr_kvs_util.h"
#include "hr_debug_util.h"
//#include "hr_reserve_stations.h"


static inline void check_w_rob_in_insert_help(context_t *ctx,
                                              ctx_trace_op_t *op,
                                              hr_w_rob_t *w_rob)
{
  if (ENABLE_ASSERTIONS) {
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    assert(keys_are_equal(&op->key, &w_rob->key));
    assert(w_rob->l_id == hr_ctx->inserted_w_id[ctx->m_id]);
    assert(w_rob->w_state == SEMIVALID);
    assert(w_rob->sess_id == op->session_id);
  }
}

static inline void fill_inv(hr_inv_t *inv,
                            ctx_trace_op_t *op,
                            hr_w_rob_t *w_rob)
{
  if (ENABLE_ASSERTIONS) assert(op->opcode == KVS_OP_PUT);
  memcpy(&inv->key, &op->key, KEY_SIZE);
  memcpy(inv->value, op->value_to_write, (size_t) VALUE_SIZE);
  inv->version = w_rob->version;

}

static inline void reset_hr_ctx_meta(context_t *ctx,
                                     uint16_t *sess_to_free,
                                     uint16_t write_num)
{
  if (write_num == 0) return;
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  hr_ctx->all_sessions_stalled = false;
  for (int w_i = 0; w_i < write_num; ++w_i) {
    uint16_t sess_id = sess_to_free[w_i];
    signal_completion_to_client(sess_id,
                                hr_ctx->index_to_req_array[sess_id],
                                ctx->t_id);
    hr_ctx->stalled[sess_id] = false;
  }
}

static inline void hr_batch_from_trace_to_KVS(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = hr_ctx->ops;
  hr_resp_t *resp = hr_ctx->resp;
  trace_t *trace = hr_ctx->trace;

  uint16_t op_i = 0;
  int working_session = -1;
  // if there are clients the "all_sessions_stalled" flag is not used,
  // so we need not bother checking it
  if (!ENABLE_CLIENTS && hr_ctx->all_sessions_stalled) {
    hr_ctx->stalled_sessions_dbg_counter++;
    if (ENABLE_ASSERTIONS) {
      if (hr_ctx->stalled_sessions_dbg_counter == MILLION) {
        //my_printf(red, "Wrkr %u, all sessions are stalled \n", ctx->t_id);
        hr_ctx->stalled_sessions_dbg_counter = 0;
      }
    }
    return;
  } else if (ENABLE_ASSERTIONS) hr_ctx->stalled_sessions_dbg_counter = 0;

  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((hr_ctx->last_session + i) % SESSIONS_PER_THREAD);
    if (pull_request_from_this_session(hr_ctx->stalled[sess_i], sess_i, ctx->t_id)) {
      working_session = sess_i;
      break;
    }
  }
  if (ENABLE_CLIENTS) {
    if (working_session == -1) return;
  }
  else if (ENABLE_ASSERTIONS) assert(working_session != -1);

  bool passed_over_all_sessions = false;

  /// main loop
  while (op_i < HR_TRACE_BATCH && !passed_over_all_sessions) {

    ctx_fill_trace_op(ctx, &trace[hr_ctx->trace_iter], &ops[op_i], working_session);
    hr_ctx->stalled[working_session] = true;
    //printf("Session %d issues a %s \n", working_session,
    // ops[op_i].opcode == KVS_OP_PUT ? "write" : "read");

    while (!pull_request_from_this_session(hr_ctx->stalled[working_session],
                                           (uint16_t) working_session, ctx->t_id)) {

      MOD_INCR(working_session, SESSIONS_PER_THREAD);
      if (working_session == hr_ctx->last_session) {
        passed_over_all_sessions = true;
        // If clients are used the condition does not guarantee that sessions are stalled
        if (!ENABLE_CLIENTS) hr_ctx->all_sessions_stalled = true;
        break;
      }
    }
    resp[op_i].type = EMPTY;
    if (!ENABLE_CLIENTS) {
      hr_ctx->trace_iter++;
      if (trace[hr_ctx->trace_iter].opcode == NOP) hr_ctx->trace_iter = 0;
    }
    op_i++;
  }
  //printf("Session %u pulled: ops %u, req_array ptr %u \n",
  //       working_session, op_i, ops[0].index_to_req_array);
  hr_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].cache_hits_per_thread += op_i;

  //uint32_t op_num = op_i + hr_ctx->buf_ops->capacity;

  hr_KVS_batch_op_trace(ctx, op_i);



  //for (uint32_t i = 0; i < op_num; i++) {
  //  ctx_trace_op_t *op = resp[i].op;
  //  // my_printf(green, "After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
  //  if (resp[i].type == KVS_MISS)  {
  //    my_printf(green, "KVS %u: bkt %u, server %u, tag %u \n", i,
  //              ops[i].key.bkt, ops[i].key.server, ops[i].key.tag);
  //    assert(false);
  //    continue;
  //  }
  //    // Local reads
  //  else if (resp[i].type == KVS_LOCAL_GET_SUCCESS) {
  //    //printf("Freeing sess %u \n", ops[i].session_id);
  //    signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, ctx->t_id);
  //    hr_ctx->all_sessions_stalled = false;
  //    hr_ctx->stalled[ops[i].session_id] = false;
  //  }
  //  else if (resp[i].type == KVS_PUT_SUCCESS) { // WRITE
  //    ctx_insert_mes(ctx, INV_QP_ID, (uint32_t) INV_SIZE, 1, false, &ops[i], 0);
  //  }
  //  else if (resp[i].type == KVS_PUT_OP_BUFFER) {
  //  }
  //}

}



///* ---------------------------------------------------------------------------
////------------------------------ COMMIT WRITES -----------------------------
////---------------------------------------------------------------------------*/


static inline void hr_commit_writes(context_t *ctx)
{
  uint16_t update_op_i = 0, local_op_i = 0;
  hr_w_rob_t *ptrs_to_w_rob[HR_UPDATE_BATCH];
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  for (int m_i = 0; m_i < MACHINE_NUM; ++m_i) {
    hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_pull_slot(&hr_ctx->w_rob[m_i]);
    while (w_rob->w_state == READY) {
      __builtin_prefetch(&w_rob->kv_ptr->seqlock, 0, 0);
      w_rob->w_state = INVALID;
      assert(update_op_i < HR_UPDATE_BATCH);
      ptrs_to_w_rob[update_op_i] = w_rob;
      //my_printf(green, "Commit sess %u write %lu, version: %lu \n",
      //          w_rob->sess_id, hr_ctx->committed_w_id[m_i] + update_op_i, w_rob->version);

      if (m_i == ctx->m_id) {
        uint16_t sess_id = w_rob->sess_id;
        assert(w_rob->acks_seen == REM_MACH_NUM);
        w_rob->acks_seen = 0;
        assert(sess_id < SESSIONS_PER_THREAD);
        assert(hr_ctx->stalled[sess_id]);
        signal_completion_to_client(sess_id,
                                    hr_ctx->index_to_req_array[sess_id],
                                    ctx->t_id);
        hr_ctx->stalled[sess_id] = false;
        local_op_i++;
      }
      fifo_incr_pull_ptr(&hr_ctx->w_rob[m_i]);
      fifo_decrem_capacity(&hr_ctx->w_rob[m_i]);
      w_rob = (hr_w_rob_t *) get_fifo_pull_slot(&hr_ctx->w_rob[m_i]);
      update_op_i++;

    }
  }

  if (update_op_i > 0) {

    for (int w_i = 0; w_i < update_op_i; ++w_i) {
      hr_w_rob_t *w_rob = ptrs_to_w_rob[w_i];
      if (w_rob->inv_applied) {
        assert(w_rob->version > 0);
        assert(w_rob != NULL);
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
static inline void insert_inv_help(context_t *ctx, void* inv_ptr,
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
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    inv_mes->l_id = hr_ctx->inserted_w_id[ctx->m_id];
    fifo_set_push_backward_ptr(send_fifo, hr_ctx->loc_w_rob->push_ptr);
  }

  if (DEBUG_INVS)
    my_printf(yellow, "Sess %u Inserting inv %lu \n",
              op->session_id, hr_ctx->inserted_w_id[ctx->m_id]);
  // Bookkeeping
  w_rob->w_state = VALID;
  fifo_incr_push_ptr(hr_ctx->loc_w_rob);
  hr_ctx->inserted_w_id[ctx->m_id]++;
  hr_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
}


static inline void send_acks_helper(context_t *ctx)
{
  ctx_refill_recvs(ctx, COM_QP_ID);
}

static inline void send_invs_helper(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[INV_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  if (DEBUG_INVS)
    printf("Wrkr %d has %u inv_mes bcasts to send credits %d\n", ctx->t_id,
           send_fifo->net_capacity, qp_meta->credits[1]);
  // Create the broadcast messages
  hr_inv_mes_t *inv_buf = (hr_inv_mes_t *) qp_meta->send_fifo->fifo;
  hr_inv_mes_t *inv_mes = &inv_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  inv_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  uint32_t backward_ptr = fifo_get_pull_backward_ptr(send_fifo);

  for (uint16_t i = 0; i < coalesce_num; i++) {
    hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_slot_mod(hr_ctx->loc_w_rob, backward_ptr + i);
    if (ENABLE_ASSERTIONS) assert(w_rob->w_state == VALID);
    w_rob->w_state = SENT;
    if (DEBUG_INVS)
      printf("inv_mes %d, version %lu, total message capacity %d\n",
             i, inv_mes->inv[i].version,
             slot_meta->byte_size);
  }

  if (DEBUG_INVS)
    my_printf(green, "Wrkr %d : I BROADCAST a inv_mes message %d of "
                "%u invs with total w_size %u,  with  credits: %d, lid: %lu  \n",
              ctx->t_id, inv_mes->opcode, coalesce_num, slot_meta->byte_size,
              qp_meta->credits[0], inv_mes->l_id);
  hr_checks_and_stats_on_bcasting_invs(ctx, coalesce_num);
}


static inline void hr_send_commits_helper(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  ctx_com_mes_t *com_mes = (ctx_com_mes_t *) get_fifo_pull_slot(send_fifo);

  if (DEBUG_COMMITS)
    my_printf(green, "Wrkr %u, Broadcasting commit %u, lid %lu, com_num %u \n",
              ctx->t_id, com_mes->opcode, com_mes->l_id, com_mes->com_num);

  hr_checks_and_stats_on_bcasting_commits(send_fifo, com_mes, ctx->t_id);
}


///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/
static inline bool inv_handler(context_t *ctx)
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
    if (ENABLE_ASSERTIONS) {
      hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_push_slot(w_rob_fifo);
      assert(w_rob->w_state == INVALID);
      w_rob->l_id = inv_mes->l_id + inv_i;
      assert(ptrs_to_inv->polled_invs < MAX_INCOMING_INV);
    }
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
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  uint64_t pull_lid = hr_ctx->committed_w_id[ctx->m_id];
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {

    if (ENABLE_ASSERTIONS && (ack_ptr == hr_ctx->loc_w_rob->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + HR_PENDING_WRITES) % HR_PENDING_WRITES;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, capacity %u \n",
                origin_ack_ptr,  (hr_ctx->loc_w_rob->pull_ptr + (ack->l_id - pull_lid)) % HR_PENDING_WRITES,
                ack_i, ack_num, hr_ctx->loc_w_rob->pull_ptr, hr_ctx->loc_w_rob->push_ptr, hr_ctx->loc_w_rob->capacity);
    }

    hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_slot(hr_ctx->loc_w_rob, ack_ptr);
    assert((w_rob->l_id % hr_ctx->w_rob->max_size) == ack_ptr);
    w_rob->acks_seen++;
    //printf("acks seen %u for %u \n", w_rob->acks_seen, ack_ptr);
    if (w_rob->acks_seen == REM_MACH_NUM) {
      if (ENABLE_ASSERTIONS) {
        qp_meta->outstanding_messages--;
        assert(w_rob->w_state == SENT);
      }
      if (DEBUG_ACKS)
        printf("Worker %d, sess %u: valid ack %u/%u write at ptr %d is ready \n",
               ctx->t_id, w_rob->sess_id, ack_i, ack_num,  ack_ptr);
      w_rob->w_state = READY;

    }
    MOD_INCR(ack_ptr, HR_PENDING_WRITES);
  }
}



static inline bool ack_handler(context_t *ctx)
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
  //hr_check_polled_ack_and_print(ack, ack_num, pull_lid, recv_fifo->pull_ptr, ctx->t_id);

  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  per_qp_meta_t *com_qp_meta = &ctx->qp_meta[COM_QP_ID];
  com_qp_meta->credits[ack->m_id] = com_qp_meta->max_credits;
  //my_printf(green, "Receiving ack for %lu, %u acks from m_id %u\n",
  // ack->l_id, ack->ack_num, ack->m_id);

  if ((hr_ctx->loc_w_rob->capacity == 0 ) ||
      (pull_lid >= l_id && (pull_lid - l_id) >= ack_num))
    return true;

  //hr_check_ack_l_id_is_small_enough(ctx, ack);
  ack_ptr = ctx_find_when_the_ack_points_acked(ack, hr_ctx->loc_w_rob, pull_lid, &ack_num);

  // Apply the acks that refer to stored writes
  hr_apply_acks(ctx, ack, ack_num, ack_ptr);
  return true;
}


static inline bool hr_commit_handler(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_com_mes_ud_t *incoming_coms = (volatile ctx_com_mes_ud_t *) recv_fifo->fifo;

  ctx_com_mes_t *com = (ctx_com_mes_t *) &incoming_coms[recv_fifo->pull_ptr].com;
  uint32_t com_num = com->com_num;

  uint64_t l_id = com->l_id;
  if (ENABLE_ASSERTIONS)
    assert(l_id == hr_ctx->committed_w_id[com->m_id]);

  hr_check_polled_commit_and_print(ctx, com, recv_fifo->pull_ptr);

  fifo_t *w_rob_fifo = &hr_ctx->w_rob[com->m_id];
  /// loop through each commit
  for (uint16_t com_i = 0; com_i < com_num; com_i++) {
    hr_w_rob_t * w_rob = get_fifo_slot_mod(w_rob_fifo, (uint32_t) (l_id + com_i));
    if (DEBUG_COMMITS)
      my_printf(yellow, "Wrkr %u, Com %u/%u, l_id %lu \n",
                ctx->t_id, com_i, com_num, l_id + com_i);

    if (ENABLE_ASSERTIONS) {
      assert(w_rob->w_state == VALID);
      assert(w_rob->l_id == l_id + com_i);
      assert(l_id + com_i == hr_ctx->committed_w_id[com->m_id]);
      assert(w_rob->m_id == com->m_id);
    }
    hr_ctx->committed_w_id[com->m_id]++;
    w_rob->w_state = READY;
  } ///

  if (ENABLE_ASSERTIONS) com->opcode = 0;

  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_coms += com_num;
    t_stats[ctx->t_id].received_coms_mes_num++;
  }

  return true;
}


static inline void main_loop(context_t *ctx)
{
  if (ctx->t_id == 0) my_printf(yellow, "Hermes main loop \n");
  while(true) {

    hr_batch_from_trace_to_KVS(ctx);
    ctx_send_broadcasts(ctx, INV_QP_ID);
    ctx_poll_incoming_messages(ctx, INV_QP_ID);
    ctx_send_acks(ctx, ACK_QP_ID);
    ctx_poll_incoming_messages(ctx, ACK_QP_ID);

    ctx_send_broadcasts(ctx, COM_QP_ID);
    ctx_poll_incoming_messages(ctx, COM_QP_ID);

    hr_commit_writes(ctx);


  }
}

#endif //ODYSSEY_HR_INLINE_UTIL_H
