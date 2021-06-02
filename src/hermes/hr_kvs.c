//
// Created by vasilis on 10/09/20.
//

#include "hr_kvs_util.h"


///* ---------------------------------------------------------------------------
////------------------------------ HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void sw_prefetch_buf_op_keys(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  if (hr_ctx->buf_ops->capacity == 0) return;
  for (int op_i = 0; op_i < hr_ctx->buf_ops->capacity; ++op_i) {
    buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_relative_slot(hr_ctx->buf_ops, op_i);
    __builtin_prefetch(buf_op->kv_ptr, 0, 0);
  }
}

static inline void init_w_rob_on_loc_inv(context_t *ctx,
                                         mica_op_t *kv_ptr,
                                         ctx_trace_op_t *op,
                                         uint64_t new_version,
                                         uint32_t write_i)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  hr_w_rob_t *w_rob = (hr_w_rob_t *)
    get_fifo_push_relative_slot(hr_ctx->loc_w_rob, write_i);
  if (ENABLE_ASSERTIONS) {
    assert(w_rob->w_state == INVALID);
    w_rob->l_id = hr_ctx->inserted_w_id[ctx->m_id];
  }
  w_rob->version = new_version;
  w_rob->kv_ptr = kv_ptr;
  w_rob->val_len = op->val_len;
  w_rob->sess_id = op->session_id;
  w_rob->w_state = SEMIVALID;
  if (ENABLE_ASSERTIONS)
    assert(hr_ctx->stalled[w_rob->sess_id]);
  if (DEBUG_INVS)
    my_printf(cyan, "W_rob insert sess %u write %lu, w_rob_i %u\n",
              w_rob->sess_id, w_rob->l_id,
              hr_ctx->loc_w_rob->push_ptr);
  fifo_increm_capacity(hr_ctx->loc_w_rob);
}



static inline void init_w_rob_on_rem_inv(context_t *ctx,
                                         mica_op_t *kv_ptr,
                                         hr_inv_mes_t *inv_mes,
                                         hr_inv_t *inv,
                                         bool inv_applied)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  hr_w_rob_t *w_rob = (hr_w_rob_t *)
    get_fifo_push_slot(&hr_ctx->w_rob[inv_mes->m_id]);
  if (DEBUG_INVS)
    my_printf(cyan, "W_rob %u for inv from %u with l_id %lu -->%lu, inserted w_id = %u\n",
              w_rob->id,  inv_mes->m_id, inv_mes->l_id,
              inv_mes->l_id + inv_mes->coalesce_num,
              hr_ctx->inserted_w_id[inv_mes->m_id]);
  if (ENABLE_ASSERTIONS) {
    assert(w_rob->w_state == INVALID);
    w_rob->l_id = hr_ctx->inserted_w_id[inv_mes->m_id];
  }
  w_rob->w_state = VALID;
  w_rob->inv_applied = inv_applied;

  hr_ctx->inserted_w_id[inv_mes->m_id]++;
  // w_rob capacity is already incremented when polling
  // to achieve back pressure at polling
  if (inv_applied) {
    w_rob->version = inv->version;
    w_rob->m_id = inv_mes->m_id;
    w_rob->kv_ptr = kv_ptr;
  }
  fifo_incr_push_ptr(&hr_ctx->w_rob[inv_mes->m_id]);
}

static inline void insert_buffered_op(context_t *ctx,
                                      mica_op_t *kv_ptr,
                                      ctx_trace_op_t *op,
                                      bool inv)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  buf_op_t *buf_op = (buf_op_t *) get_fifo_push_slot(hr_ctx->buf_ops);
  buf_op->op.opcode = op->opcode;
  buf_op->op.key = op->key;
  buf_op->op.session_id = op->session_id;
  buf_op->op.index_to_req_array = op->index_to_req_array;
  buf_op->kv_ptr = kv_ptr;


  if (inv) {
    buf_op->op.value_to_write = op->value_to_write;
  }
  else {
    buf_op->op.value_to_read = op->value_to_read;
  }

  fifo_incr_push_ptr(hr_ctx->buf_ops);
  fifo_increm_capacity(hr_ctx->buf_ops);
}

///* ---------------------------------------------------------------------------
////------------------------------ REQ PROCESSING -----------------------------
////---------------------------------------------------------------------------*/

static inline void hr_local_inv(context_t *ctx,
                                mica_op_t *kv_ptr,
                                ctx_trace_op_t *op,
                                uint32_t *write_i)
{
  bool success = false;
  uint64_t new_version;
  read_seqlock_lock_free(&kv_ptr->seqlock);
  if (kv_ptr->state != HR_W &&
      kv_ptr->state != HR_INV_T) {
    lock_seqlock(&kv_ptr->seqlock);
    {
      if (kv_ptr->state != HR_W &&
          kv_ptr->state != HR_INV_T) {
        kv_ptr->state = HR_W;
        kv_ptr->version++;
        new_version = kv_ptr->version;
        kv_ptr->m_id = ctx->m_id;
        memcpy(kv_ptr->value, op->value_to_write, VALUE_SIZE);
        success = true;
      }
    }
    unlock_seqlock(&kv_ptr->seqlock);
  }

  if (success) {
    init_w_rob_on_loc_inv(ctx, kv_ptr, op, new_version, *write_i);
    if (INSERT_WRITES_FROM_KVS)
      od_insert_mes(ctx, INV_QP_ID, (uint32_t) INV_SIZE, 1, false, op, 0, 0);
    else {
      hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
      hr_ctx->ptrs_to_inv->ptr_to_ops[*write_i] = (hr_inv_t *) op;
      (*write_i)++;
    }
  }
  else {
    insert_buffered_op(ctx, kv_ptr, op, true);
  }
}

static inline void hr_rem_inv(context_t *ctx,
                              mica_op_t *kv_ptr,
                              hr_inv_mes_t *inv_mes,
                              hr_inv_t *inv)
{
  bool inv_applied = false;
  compare_t comp = compare_flat_ts(inv->version, inv_mes->m_id,
                                   kv_ptr->version, kv_ptr->m_id);

  if (comp ==  GREATER) {
    lock_seqlock(&kv_ptr->seqlock);
    comp = compare_flat_ts(inv->version, inv_mes->m_id,
                           kv_ptr->version, kv_ptr->m_id);
    if (comp == GREATER) {
      if (kv_ptr->state != HR_W) {
        kv_ptr->state = HR_INV;
      }
      else kv_ptr->state = HR_INV_T;
      kv_ptr->version = inv->version;
      kv_ptr->m_id = inv_mes->m_id;
      memcpy(kv_ptr->value, inv->value, VALUE_SIZE);
      inv_applied = true;
    }
    unlock_seqlock(&kv_ptr->seqlock);
  }

  init_w_rob_on_rem_inv(ctx, kv_ptr, inv_mes, inv, inv_applied);
}


static inline void hr_loc_read(context_t *ctx,
                               mica_op_t *kv_ptr,
                               ctx_trace_op_t *op)
{
  if (ENABLE_ASSERTIONS) {
    assert(op->value_to_read != NULL);
    assert(kv_ptr != NULL);
  }
  bool success = false;
  uint32_t debug_cntr = 0;
  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "local read", ctx->t_id);
    if (kv_ptr->state == HR_V) {
      memcpy(op->value_to_read, kv_ptr->value, (size_t) VALUE_SIZE);
      success = true;
    }
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  if (success) {
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    signal_completion_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
    hr_ctx->all_sessions_stalled = false;
    hr_ctx->stalled[op->session_id] = false;
  }
  else insert_buffered_op(ctx, kv_ptr, op, false);
}


static inline void handle_trace_reqs(context_t *ctx,
                                     mica_op_t *kv_ptr,
                                     ctx_trace_op_t *op,
                                     uint32_t *write_i,
                                     uint16_t op_i)
{
  if (op->opcode == KVS_OP_GET) {
    hr_loc_read(ctx, kv_ptr, op);
  }
  else if (op->opcode == KVS_OP_PUT) {
    hr_local_inv(ctx, kv_ptr, op,  write_i);

  }
  else if (ENABLE_ASSERTIONS) {
    my_printf(red, "wrong Opcode in cache: %d, req %d \n", op->opcode, op_i);
    assert(0);
  }
}

///* ---------------------------------------------------------------------------
////------------------------------ KVS_API -----------------------------
////---------------------------------------------------------------------------*/

inline void hr_KVS_batch_op_trace(context_t *ctx, uint16_t op_num)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *op = hr_ctx->ops;
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(op != NULL);
    assert(op_num > 0 && op_num <= HR_TRACE_BATCH);
  }

  unsigned int bkt[HR_TRACE_BATCH];
  struct mica_bkt *bkt_ptr[HR_TRACE_BATCH];
  unsigned int tag[HR_TRACE_BATCH];
  mica_op_t *kv_ptr[HR_TRACE_BATCH];	/* Ptr to KV item in log */


  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  uint32_t buf_ops_num = hr_ctx->buf_ops->capacity;
  uint32_t write_i = 0;

  //sw_prefetch_buf_op_keys(ctx);

  for (op_i = 0; op_i < buf_ops_num; ++op_i) {
    buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_slot(hr_ctx->buf_ops);
    check_state_with_allowed_flags(3, buf_op->op.opcode, KVS_OP_PUT, KVS_OP_GET);
    handle_trace_reqs(ctx, buf_op->kv_ptr, &buf_op->op, &write_i, op_i);
    fifo_incr_pull_ptr(hr_ctx->buf_ops);
    fifo_decrem_capacity(hr_ctx->buf_ops);
  }

  for(op_i = 0; op_i < op_num; op_i++) {
    od_KVS_check_key(kv_ptr[op_i], op[op_i].key, op_i);
    handle_trace_reqs(ctx, kv_ptr[op_i], &op[op_i], &write_i, op_i);
  }
  if (!INSERT_WRITES_FROM_KVS)
    hr_ctx->ptrs_to_inv->polled_invs = (uint16_t) write_i;
}

inline void hr_KVS_batch_op_invs(context_t *ctx)
{
  hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
  ptrs_to_inv_t *ptrs_to_inv = hr_ctx->ptrs_to_inv;
  hr_inv_mes_t **inv_mes = hr_ctx->ptrs_to_inv->ptr_to_mes;
  hr_inv_t **invs = ptrs_to_inv->ptr_to_ops;
  uint16_t op_num = ptrs_to_inv->polled_invs;

  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(invs != NULL);
    assert(op_num > 0 && op_num <= MAX_INCOMING_INV);
  }

  unsigned int bkt[MAX_INCOMING_INV];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_INV];
  unsigned int tag[MAX_INCOMING_INV];
  mica_op_t *kv_ptr[MAX_INCOMING_INV];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &invs[op_i]->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    od_KVS_check_key(kv_ptr[op_i], invs[op_i]->key, op_i);
    hr_rem_inv(ctx, kv_ptr[op_i], inv_mes[op_i], invs[op_i]);
  }
}