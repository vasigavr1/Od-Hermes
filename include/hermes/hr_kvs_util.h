//
// Created by vasilis on 08/09/20.
//

#ifndef ODYSSEY_HR_KVS_UTIL_H
#define ODYSSEY_HR_KVS_UTIL_H

#include <network_context.h>
#include <netw_func.h>
#include "kvs.h"
#include "hr_config.h"


void hr_KVS_batch_op_trace(context_t *ctx, uint16_t op_num);

void hr_KVS_batch_op_invs(context_t *ctx);

#endif //ODYSSEY_HR_KVS_UTIL_H
