//
// Created by vasilis on 08/09/20.
//

#ifndef ODYSSEY_HR_DEBUG_UTIL_H
#define ODYSSEY_HR_DEBUG_UTIL_H

#include "hr_config.h"

static inline void hr_checks_and_stats_on_bcasting_invs(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[INV_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  hr_inv_mes_t *inv_buf = (hr_inv_mes_t *) qp_meta->send_fifo->fifo;
  hr_inv_mes_t *inv_mes = &inv_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(inv_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    uint32_t backward_ptr = fifo_get_pull_backward_ptr(send_fifo);
    if (DEBUG_INVS)
      printf("Wrkr %d has %u inv_mes bcasts to send credits %d\n", ctx->t_id,
             send_fifo->net_capacity, qp_meta->credits[1]);
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
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].invs_sent +=
      coalesce_num;
    t_stats[ctx->t_id].inv_sent_mes_num++;
  }
}




static inline void hr_check_polled_inv_and_print(context_t *ctx,
                                                 hr_inv_mes_t* inv_mes)
{

  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[INV_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (DEBUG_INVS)
    my_printf(green, "Wrkr %d sees a inv_mes message "
                "with %d invs at index %u l_id %u from machine %u\n",
              ctx->t_id, inv_mes->coalesce_num, recv_fifo->pull_ptr,
              inv_mes->l_id, inv_mes->m_id);
  if (ENABLE_ASSERTIONS) {
    assert(inv_mes->m_id < MACHINE_NUM);
    assert(inv_mes->m_id != ctx->m_id);
    assert(inv_mes->opcode == KVS_OP_PUT);
    assert(inv_mes->coalesce_num > 0 && inv_mes->coalesce_num <= INV_COALESCE);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_invs += inv_mes->coalesce_num;
    t_stats[ctx->t_id].received_invs_mes_num++;
  }
}


static inline void hr_checks_and_stats_on_bcasting_commits(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  ctx_com_mes_t *com_mes = (ctx_com_mes_t *) get_fifo_pull_slot(send_fifo);
  if (DEBUG_COMMITS)
    my_printf(green, "Wrkr %u, Broadcasting commit %u, lid %lu, com_num %u \n",
              ctx->t_id, com_mes->opcode, com_mes->l_id, com_mes->com_num);
  if (ENABLE_ASSERTIONS) {
    assert(com_mes->com_num == get_fifo_slot_meta_pull(send_fifo)->coalesce_num);
    assert(send_fifo->net_capacity >= com_mes->com_num);

    assert(send_fifo != NULL);
    if (send_fifo->capacity > COMMIT_FIFO_SIZE)
      printf("com fifo capacity %u/%d \n", send_fifo->capacity, COMMIT_FIFO_SIZE);
    assert(send_fifo->capacity <= COMMIT_FIFO_SIZE);
    assert(com_mes->com_num > 0 && com_mes->com_num <= MAX_LIDS_IN_A_COMMIT);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].coms_sent += com_mes->com_num;
    t_stats[ctx->t_id].coms_sent_mes_num++;
  }
}

static inline void hr_check_polled_commit_and_print(context_t *ctx,
                                                    ctx_com_mes_t *com,
                                                    uint32_t buf_ptr)
{
  if (ENABLE_ASSERTIONS) {
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    assert(com->m_id < MACHINE_NUM && com->m_id != ctx->m_id);
    if (DEBUG_COMMITS)
      my_printf(yellow, "Wrkr %d com opcode %d from machine %u, with %d coms for l_id %lu, waiting %u"
                  " at offset %d at address %p \n",
                ctx->t_id, com->opcode, com->m_id, com->com_num, com->l_id,
                hr_ctx->committed_w_id[com->m_id],
                buf_ptr, (void *) com);
    if (ENABLE_ASSERTIONS) {
      assert(com->opcode == COMMIT_OP);
      assert(com->com_num > 0 && com->com_num <= MAX_LIDS_IN_A_COMMIT);
    }
  }

  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_coms += com->com_num;
    t_stats[ctx->t_id].received_coms_mes_num++;
  }
}


static inline void check_w_rob_in_insert_help(context_t *ctx,
                                              ctx_trace_op_t *op,
                                              hr_w_rob_t *w_rob)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  if (DEBUG_INVS)
    my_printf(yellow, "Sess %u Inserting inv %lu \n",
              op->session_id, hr_ctx->inserted_w_id[ctx->m_id]);
  if (ENABLE_ASSERTIONS) {
    assert(w_rob->l_id == hr_ctx->inserted_w_id[ctx->m_id]);
    assert(w_rob->w_state == SEMIVALID);
    assert(w_rob->sess_id == op->session_id);
  }
}

static inline void check_w_rob_when_handling_an_inv(ptrs_to_inv_t *ptrs_to_inv,
                                                    fifo_t *w_rob_fifo,
                                                    hr_inv_mes_t *inv_mes,
                                                    uint8_t inv_i)
{
  if (ENABLE_ASSERTIONS) {
    hr_w_rob_t *w_rob = (hr_w_rob_t *) get_fifo_push_relative_slot(w_rob_fifo, inv_i);
    assert(w_rob->w_state == INVALID);
    w_rob->l_id = inv_mes->l_id + inv_i;
    assert(ptrs_to_inv->polled_invs < MAX_INCOMING_INV);
  }
}


static inline void check_when_applying_acks(context_t *ctx,
                                            hr_w_rob_t *w_rob,
                                            ctx_ack_mes_t *ack,
                                            uint32_t ack_num,
                                            uint32_t ack_ptr,
                                            uint16_t ack_i)
{
  if (ENABLE_ASSERTIONS) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    uint64_t pull_lid = hr_ctx->committed_w_id[ctx->m_id];
    if (ENABLE_ASSERTIONS && (ack_ptr == hr_ctx->loc_w_rob->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + HR_PENDING_WRITES) % HR_PENDING_WRITES;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, capacity %u \n",
                origin_ack_ptr, (hr_ctx->loc_w_rob->pull_ptr + (ack->l_id - pull_lid)) % HR_PENDING_WRITES,
                ack_i, ack_num, hr_ctx->loc_w_rob->pull_ptr, hr_ctx->loc_w_rob->push_ptr, hr_ctx->loc_w_rob->capacity);
    }

    assert((w_rob->l_id % hr_ctx->w_rob->max_size) == ack_ptr);
    if (w_rob->acks_seen == REM_MACH_NUM) {
      qp_meta->outstanding_messages--;
      assert(w_rob->w_state == SENT);
      if (DEBUG_ACKS)
        printf("Worker %d, sess %u: valid ack %u/%u write at ptr %d is ready \n",
               ctx->t_id, w_rob->sess_id, ack_i, ack_num,  ack_ptr);
    }
  }
}


static inline void hr_check_each_commit(context_t *ctx,
                                        ctx_com_mes_t *com,
                                        hr_w_rob_t *w_rob,
                                        uint16_t com_i)
{
  uint32_t com_num = com->com_num;
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  uint64_t l_id = com->l_id;

  if (DEBUG_COMMITS)
    my_printf(yellow, "Wrkr %u, Com %u/%u, l_id %lu \n",
              ctx->t_id, com_i, com_num, l_id + com_i);

  if (ENABLE_ASSERTIONS) {
    assert(w_rob->w_state == VALID);
    assert(w_rob->l_id == l_id + com_i);
    assert(l_id + com_i == hr_ctx->committed_w_id[com->m_id]);
    assert(w_rob->m_id == com->m_id);
  }
}


#endif //ODYSSEY_HR_DEBUG_UTIL_H
