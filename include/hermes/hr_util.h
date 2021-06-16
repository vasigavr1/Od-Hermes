//
// Created by vasilis on 07/09/20.
//

#ifndef ODYSSEY_HR_UTIL_H
#define ODYSSEY_HR_UTIL_H


#include "hr_config.h"
#include "od_network_context.h"
#include <hr_inline_util.h>
#include "../../../odlib/include/trace/od_trace_util.h"
#include "od_init_func.h"

void hr_stats(stats_ctx_t *ctx);


void hr_init_functionality(int argc, char *argv[]);
void hr_init_qp_meta(context_t *ctx);
void* set_up_hr_ctx(context_t *ctx);



typedef struct stats {
  double batch_size_per_thread[WORKERS_PER_MACHINE];
  double com_batch_size[WORKERS_PER_MACHINE];
  double inv_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
  double stalled_gid[WORKERS_PER_MACHINE];
  double stalled_ack_inv[WORKERS_PER_MACHINE];
  double stalled_com_credit[WORKERS_PER_MACHINE];


  double total_reqs[WORKERS_PER_MACHINE];


  double invs_sent[WORKERS_PER_MACHINE];
  double acks_sent[WORKERS_PER_MACHINE];
  double coms_sent[WORKERS_PER_MACHINE];

  double received_coms[WORKERS_PER_MACHINE];
  double received_acks[WORKERS_PER_MACHINE];
  double received_invs[WORKERS_PER_MACHINE];

  double write_ratio_per_client[WORKERS_PER_MACHINE];
} all_stats_t;
#endif //ODYSSEY_HR_UTIL_H
