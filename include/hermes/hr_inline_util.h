//
// Created by vasilis on 07/09/20.
//

#ifndef ODYSSEY_HR_INLINE_UTIL_H
#define ODYSSEY_HR_INLINE_UTIL_H


#include <od_inline_util.h>
#include "od_network_context.h"
#include "hr_kvs_util.h"
#include "hr_debug_util.h"
//#include "hr_reserve_stations.h"



///* ---------------------------------------------------------------------------
////------------------------------SEND HELPERS -----------------------------
////---------------------------------------------------------------------------*/

void insert_inv_help(context_t *ctx, void* inv_ptr,
                                    void *source, uint32_t source_flag);
void send_acks_helper(context_t *ctx);

void send_invs_helper(context_t *ctx);

void hr_send_commits_helper(context_t *ctx);


///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/
bool inv_handler(context_t *ctx);
bool ack_handler(context_t *ctx);
bool hr_commit_handler(context_t *ctx);

///* ---------------------------------------------------------------------------
////------------------------------MAIN LOOP -----------------------------
////---------------------------------------------------------------------------*/

_Noreturn void hr_main_loop(context_t *ctx);

#endif //ODYSSEY_HR_INLINE_UTIL_H
