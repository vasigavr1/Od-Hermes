//
// Created by vasilis on 07/09/20.
//

#ifndef ODYSSEY_HR_MESSAGES_H
#define ODYSSEY_HR_MESSAGES_H


#define VAL_CREDITS 10
#define MAX_VAL_SIZE 500

#define MAX_VAL_WRS (MESSAGES_IN_BCAST_BATCH)

#define MAX_VAL_BUF_SLOTS_TO_BE_POLLED ( VAL_CREDITS * REM_MACH_NUM)
#define MAX_RECV_VAL_WRS (VAL_CREDITS * REM_MACH_NUM)
#define VAL_BUF_SLOTS (MAX_RECV_VAL_WRS)

#define VAL_MES_HEADER 12 // opcode(1), coalesce_num(1) l_id (8)
#define EFFECTIVE_MAX_VAL_SIZE (MAX_VAL_SIZE - VAL_MES_HEADER)
#define VAL_SIZE (16 + VALUE_SIZE)
#define VAL_COALESCE (EFFECTIVE_MAX_VAL_SIZE / VAL_SIZE)
#define VAL_SEND_SIZE (VAL_MES_HEADER + (VAL_COALESCE * VAL_SIZE))
#define VAL_RECV_SIZE (GRH_SIZE + VAL_SEND_SIZE)

#define VAL_FIFO_SIZE (SESSIONS_PER_THREAD + 1)
#define COMMIT_FIFO_SIZE 1

typedef struct hr_val {
  uint64_t version;
  mica_key_t key;
  //uint8_t val_len;
  //uint8_t opcode; //override opcode
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) hr_val_t;

// val message
typedef struct hr_val_message {
  uint64_t l_id;
  uint8_t opcode;
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t unused;
  hr_val_t val[VAL_COALESCE];
} __attribute__((__packed__)) hr_val_mes_t;

typedef struct hr_val_message_ud_req {
  uint8_t grh[GRH_SIZE];
  hr_val_mes_t val;
} hr_val_mes_ud_t;


#define COM_CREDITS 10
#define COM_WRS MESSAGES_IN_BCAST_BATCH
#define RECV_COM_WRS (REM_MACH_NUM * COM_CREDITS)
#define COM_BUF_SLOTS RECV_COM_WRS
#define MAX_LIDS_IN_A_COMMIT SESSIONS_PER_THREAD

#endif //ODYSSEY_HR_MESSAGES_H
