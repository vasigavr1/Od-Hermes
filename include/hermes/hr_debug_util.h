//
// Created by vasilis on 08/09/20.
//

#ifndef ODYSSEY_HR_DEBUG_UTIL_H
#define ODYSSEY_HR_DEBUG_UTIL_H

#include "hr_config.h"

static inline void hr_checks_and_stats_on_bcasting_invs(context_t *ctx,
                                                        uint8_t coalesce_num)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[INV_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
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


static inline void hr_checks_and_stats_on_bcasting_commits(fifo_t *send_fifo,
                                                           ctx_com_mes_t *com_mes,
                                                           uint16_t t_id)
{
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
    t_stats[t_id].coms_sent += com_mes->com_num;
    t_stats[t_id].coms_sent_mes_num++;
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
}

#endif //ODYSSEY_HR_DEBUG_UTIL_H
