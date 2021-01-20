//
// Created by vasilis on 07/09/20.
//

#ifndef ODYSSEY_HR_MESSAGES_H
#define ODYSSEY_HR_MESSAGES_H


#define INV_CREDITS 3
#define INV_COALESCE 20
//#define MAX_INV_SIZE 1212

#define MAX_INV_WRS (MESSAGES_IN_BCAST_BATCH)

#define MAX_INV_BUF_SLOTS_TO_BE_POLLED ( INV_CREDITS * REM_MACH_NUM)
#define MAX_RECV_INV_WRS (INV_CREDITS * REM_MACH_NUM)
#define INV_BUF_SLOTS (MAX_RECV_INV_WRS)

#define INV_MES_HEADER 12 // opcode(1), coalesce_num(1) l_id (8)
//#define EFFECTIVE_MAX_INV_SIZE (MAX_INV_SIZE - INV_MES_HEADER)
#define INV_SIZE (16 + VALUE_SIZE)
//#define INV_COALESCE (EFFECTIVE_MAX_INV_SIZE / INV_SIZE)
#define INV_SEND_SIZE (INV_MES_HEADER + (INV_COALESCE * INV_SIZE))
#define INV_RECV_SIZE (GRH_SIZE + INV_SEND_SIZE)

#define INV_FIFO_SIZE (SESSIONS_PER_THREAD + 1)
#define COMMIT_FIFO_SIZE 1

typedef struct hr_inv {
  uint64_t version;
  mica_key_t key;
  //uint8_t val_len;
  //uint8_t opcode; //override opcode
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) hr_inv_t;

// val message
typedef struct hr_inv_message {
  uint64_t l_id;
  uint8_t opcode;
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t unused;
  hr_inv_t inv[INV_COALESCE];
} __attribute__((__packed__)) hr_inv_mes_t;

typedef struct hr_inv_message_ud_req {
  uint8_t grh[GRH_SIZE];
  hr_inv_mes_t inv_mes;
} hr_inv_mes_ud_t;


#define COM_CREDITS 10
#define COM_WRS MESSAGES_IN_BCAST_BATCH
#define RECV_COM_WRS (REM_MACH_NUM * COM_CREDITS)
#define COM_BUF_SLOTS RECV_COM_WRS
#define MAX_LIDS_IN_A_COMMIT SESSIONS_PER_THREAD

#endif //ODYSSEY_HR_MESSAGES_H
