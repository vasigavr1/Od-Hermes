//
// Created by vasilis on 07/09/20.
//

#ifndef ODYSSEY_HR_CONFIG_H
#define ODYSSEY_HR_CONFIG_H


#include <fifo.h>
#include <hr_messages.h>
#include <network_context.h>

#define HR_W_ROB_SIZE SESSIONS_PER_THREAD
#define HR_TRACE_BATCH SESSIONS_PER_THREAD
#define HR_PENDING_WRITES (SESSIONS_PER_THREAD + 1)
#define HR_UPDATE_BATCH (HR_PENDING_WRITES * MACHINE_NUM)
#define MAX_INCOMING_INV (REM_MACH_NUM * HR_W_ROB_SIZE)

#define QP_NUM 3
#define INV_QP_ID 0
#define ACK_QP_ID 1
#define COM_QP_ID 2

#define  VAL_MCAST_QP 0
#define COM_MCAST_QP 1

/*------------------------------------------------
 * ----------------KVS----------------------------
 * ----------------------------------------------*/
#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (32 + ((MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))
#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)


typedef enum{HR_V = 0, HR_INV, HR_INV_T, HR_W} key_state_t;

struct mica_op {
  // Cache-line -1
  uint8_t value[MICA_VALUE_SIZE];
  // Cache-line -2
  struct key key;
  seqlock_t seqlock;
  uint64_t version;
  uint8_t m_id;
  uint8_t state;
  uint8_t unused[2];
  uint32_t key_id; // strictly for debug
  uint8_t padding[MICA_OP_PADDING_SIZE];
};



/*------------------------------------------------
 * ----------------TRACE----------------------------
 * ----------------------------------------------*/
typedef struct hr_resp {
  uint8_t type;
} hr_resp_t;


/*------------------------------------------------
 * ----------------RESERVATION STATIONS-----------
 * ----------------------------------------------*/
typedef enum op_state {INVALID, SEMIVALID, VALID, SENT, READY, SEND_COMMITTS} w_state_t;
//typedef enum {NOT_USED, LOCAL_PREP, REMOTE_WRITE} source_t;
//typedef struct ptrs_to_reads {
//  uint16_t polled_reads;
//  hr_read_t **ptr_to_ops;
//  hr_r_mes_t **ptr_to_r_mes;
//  bool *coalesce_r_rep;
//} ptrs_to_r_t;

typedef struct ptrs_to_invs {
  uint16_t polled_invs;
  hr_inv_t **ptr_to_ops;
  hr_inv_mes_t **ptr_to_mes;
} ptrs_to_inv_t;


typedef struct w_rob {
  uint64_t version;
  uint64_t l_id; // TODO not needed
  mica_op_t *kv_ptr;
  mica_key_t key; // TODO not needed

  uint16_t sess_id;

  w_state_t w_state;
  uint8_t m_id;
  uint8_t acks_seen;
  uint8_t val_len;
  //uint32_t index_to_req_array;
  bool is_local;
  bool inv_applied;
  //hr_prepare_t *ptr_to_op;

  //uint8_t value[VALUE_SIZE];

} w_rob_t;




// A data structute that keeps track of the outstanding writes
typedef struct hr_ctx {
  // reorder buffers
  // One fifo per machine in the configuration
  fifo_t *w_rob;
  fifo_t *loc_w_rob; //points in the w_rob

  ptrs_to_inv_t *ptrs_to_inv;

  trace_t *trace;
  uint32_t trace_iter;
  uint16_t last_session;

  ctx_trace_op_t *ops;
  hr_resp_t *resp;

  //ptrs_to_r_t *ptrs_to_r;
  uint64_t *inserted_w_id;
  uint64_t *committed_w_id;

  uint64_t local_r_id;

  uint32_t *index_to_req_array; // [SESSIONS_PER_THREAD]
  bool *stalled;

  bool all_sessions_stalled;

  uint32_t wait_for_gid_dbg_counter;
  uint32_t stalled_sessions_dbg_counter;
} hr_ctx_t;


typedef struct thread_stats { // 2 cache lines
  long long cache_hits_per_thread;
  long long remotes_per_client;
  long long locals_per_client;

  long long invs_sent;
  long long acks_sent;
  long long coms_sent;
  long long writes_sent;
  uint64_t reads_sent;

  long long inv_sent_mes_num;
  long long acks_sent_mes_num;
  long long coms_sent_mes_num;
  long long writes_sent_mes_num;
  uint64_t reads_sent_mes_num;


  long long received_coms;
  long long received_acks;
  long long received_invs;
  long long received_writes;

  long long received_coms_mes_num;
  long long received_acks_mes_num;
  long long received_invs_mes_num;
  long long received_writes_mes_num;


  uint64_t batches_per_thread; // Leader only
  uint64_t total_writes; // Leader only

  uint64_t stalled_gid;
  uint64_t stalled_ack_inv;
  uint64_t stalled_com_credit;
  //long long unused[3]; // padding to avoid false sharing
} t_stats_t;

extern t_stats_t t_stats[WORKERS_PER_MACHINE];

#endif //ODYSSEY_HR_CONFIG_H
