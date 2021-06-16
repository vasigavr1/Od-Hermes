//
// Created by vasilis on 07/09/20.
//

#include "hr_util.h"

void hr_stats(stats_ctx_t *ctx)
{

  double seconds = ctx-> seconds;
  uint16_t print_count = ctx->print_count;
  t_stats_t *curr_w_stats = ctx->curr_w_stats;
  t_stats_t *prev_w_stats = ctx->prev_w_stats;
  all_stats_t all_stats;


  double total_throughput = 0;
  uint64_t all_clients_cache_hits = 0;
  seconds *= MILLION; // compute only MIOPS
  for (int i = 0; i < num_threads; i++) {
    all_clients_cache_hits += curr_w_stats[i].total_reqs - prev_w_stats[i].total_reqs;
    all_stats.total_reqs[i] =
      (curr_w_stats[i].total_reqs - prev_w_stats[i].total_reqs) / seconds;

    all_stats.stalled_gid[i] = (curr_w_stats[i].stalled_gid - prev_w_stats[i].stalled_gid) / seconds;
    all_stats.stalled_ack_inv[i] = (curr_w_stats[i].stalled_ack_inv - prev_w_stats[i].stalled_ack_inv) / seconds;
    all_stats.stalled_com_credit[i] =
      (curr_w_stats[i].stalled_com_credit - prev_w_stats[i].stalled_com_credit) / seconds;

    all_stats.invs_sent[i] = (curr_w_stats[i].invs_sent - prev_w_stats[i].invs_sent) / seconds;
    all_stats.coms_sent[i] = (curr_w_stats[i].coms_sent - prev_w_stats[i].coms_sent) / seconds;
    all_stats.acks_sent[i] = (curr_w_stats[i].acks_sent - prev_w_stats[i].acks_sent) / seconds;
    all_stats.received_coms[i] = (curr_w_stats[i].received_coms - prev_w_stats[i].received_coms) / seconds;
    all_stats.received_invs[i] = (curr_w_stats[i].received_invs - prev_w_stats[i].received_invs) / seconds;
    all_stats.received_acks[i] = (curr_w_stats[i].received_acks - prev_w_stats[i].received_acks) / seconds;

    all_stats.batch_size_per_thread[i] = (curr_w_stats[i].total_writes - prev_w_stats[i].total_writes) /
                                         (double) (curr_w_stats[i].batches_per_thread -
                                                   prev_w_stats[i].batches_per_thread);
    all_stats.com_batch_size[i] = (curr_w_stats[i].coms_sent - prev_w_stats[i].coms_sent) /
                                  (double) (curr_w_stats[i].coms_sent_mes_num -
                                            prev_w_stats[i].coms_sent_mes_num);
    all_stats.inv_batch_size[i] = (curr_w_stats[i].invs_sent - prev_w_stats[i].invs_sent) /
                                   (double) (curr_w_stats[i].inv_sent_mes_num -
                                             prev_w_stats[i].inv_sent_mes_num);

    all_stats.ack_batch_size[i] = (curr_w_stats[i].acks_sent - prev_w_stats[i].acks_sent) /
                                  (double) (curr_w_stats[i].acks_sent_mes_num -
                                            prev_w_stats[i].acks_sent_mes_num);
    all_stats.write_batch_size[i] = (curr_w_stats[i].writes_sent - prev_w_stats[i].writes_sent) /
                                    (double) (curr_w_stats[i].writes_sent_mes_num -
                                              prev_w_stats[i].writes_sent_mes_num);

  }

  memcpy(prev_w_stats, curr_w_stats, num_threads * (sizeof(struct thread_stats)));
  total_throughput = (all_clients_cache_hits) / seconds;

  if (!SHOW_AGGREGATE_STATS) {
    printf("---------------PRINT %d time elapsed %.2f---------------\n", print_count, seconds / MILLION);
    my_printf(green, "SYSTEM MIOPS: %.2f \n", total_throughput);
    for (int i = 0; i < num_threads; i++) {
      my_printf(cyan, "T%d: ", i);
      my_printf(yellow, "%.2f MIOPS, STALL: GID: %.2f/s, ACK/inv %.2f/s, COM/CREDIT %.2f/s", i,
                all_stats.total_reqs[i],
                all_stats.stalled_gid[i],
                all_stats.stalled_ack_inv[i],
                all_stats.stalled_com_credit[i]);

      my_printf(yellow, ", BATCHES: GID %.2f, Coms %.2f, invs %.2f ",
                all_stats.batch_size_per_thread[i],
                all_stats.com_batch_size[i],
                all_stats.inv_batch_size[i]);

      my_printf(yellow, ", BATCHES: Acks %.2f, Ws %.2f ",
                all_stats.ack_batch_size[i],
                all_stats.write_batch_size[i]);

      //if (i > 0 && i % 2 == 0)
      printf("\n");
    }
    printf("\n");
    printf("---------------------------------------\n");
  }
  my_printf(green, "%u. SYSTEM MIOPS: %.2f \n", print_count, total_throughput);
}