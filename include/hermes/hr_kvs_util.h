//
// Created by vasilis on 08/09/20.
//

#ifndef ODYSSEY_HR_KVS_UTIL_H
#define ODYSSEY_HR_KVS_UTIL_H

#include <network_context.h>
#include "kvs.h"
#include "hr_config.h"

static inline void hr_local_inv(context_t *ctx,
                                mica_op_t *kv_ptr,
                                ctx_trace_op_t *op,
                                hr_resp_t *resp,
                                uint32_t write_i)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  w_rob_t *w_rob = (w_rob_t *)
    get_fifo_push_relative_slot(hr_ctx->loc_w_rob, write_i);
  uint64_t new_version;
  lock_seqlock(&kv_ptr->seqlock);
  kv_ptr->state = HR_INV;
  kv_ptr->version++;
  new_version = kv_ptr->version;
  kv_ptr->m_id = ctx->m_id;
  memcpy(kv_ptr->value, op->value,  VALUE_SIZE);
  unlock_seqlock(&kv_ptr->seqlock);

  if (ENABLE_ASSERTIONS)
    assert(w_rob->w_state == INVALID);
  w_rob->key = op->key;
  w_rob->version = new_version;
  w_rob->kv_ptr = kv_ptr;


  w_rob->l_id = hr_ctx->inserted_w_id + write_i;

  w_rob->val_len = op->val_len;
  w_rob->sess_id = op->session_id;
  w_rob->w_state = SEMIVALID;

  if (ENABLE_ASSERTIONS)
    assert(hr_ctx->stalled[w_rob->sess_id]);
  //my_printf(cyan, "W_rob insert sess %u write %lu, w_rob_i %u\n",
  //          w_rob->sess_id, w_rob->l_id,
  //          (hr_ctx->loc_w_rob->push_ptr + write_i) % hr_ctx->loc_w_rob->max_size);

  resp->type = KVS_PUT_SUCCESS;
  fifo_incr_capacity(hr_ctx->loc_w_rob);
}


static inline void hr_KVS_batch_op_trace(context_t *ctx, uint16_t op_num)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *op = hr_ctx->ops;
  hr_resp_t *resp = hr_ctx->resp;
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(op != NULL);
    assert(op_num > 0 && op_num <= HR_TRACE_BATCH);
    assert(resp != NULL);
  }

  unsigned int bkt[HR_TRACE_BATCH];
  struct mica_bkt *bkt_ptr[HR_TRACE_BATCH];
  unsigned int tag[HR_TRACE_BATCH];
  mica_op_t *kv_ptr[HR_TRACE_BATCH];	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @op_i loops work
   * for both GETs and PUTs.
   */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  //uint32_t r_push_ptr = hr_ctx->r_rob->push_ptr;
  // the following variables used to validate atomicity between a lock-free read of an object
  uint32_t write_i = 0;
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_check_key(kv_ptr[op_i], op[op_i].key, op_i);
    if (op[op_i].opcode == KVS_OP_GET ) {
      KVS_local_read(kv_ptr[op_i], op[op_i].value_to_read, &resp[op_i].type, ctx->t_id);
    }
    else if (op[op_i].opcode == KVS_OP_PUT) {
      hr_local_inv(ctx, kv_ptr[op_i], &op[op_i], &resp[op_i], write_i);
      write_i++;
    }
    else if (ENABLE_ASSERTIONS) {
      my_printf(red, "wrong Opcode in cache: %d, req %d \n", op[op_i].opcode, op_i);
      assert(0);
    }
  }
}


#endif //ODYSSEY_HR_KVS_UTIL_H
