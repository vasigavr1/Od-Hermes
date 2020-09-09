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
    assert(inv_mes->opcode == KVS_OP_PUT);
    assert(inv_mes->coalesce_num > 0 && inv_mes->coalesce_num <= INV_COALESCE);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_invs += inv_mes->coalesce_num;
    t_stats[ctx->t_id].received_invs_mes_num++;
  }
}
#endif //ODYSSEY_HR_DEBUG_UTIL_H
