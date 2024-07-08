#include "logger.h"
#include "work_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include <fstream>


void Logger::init(const char * log_file_name) {
  this->log_file_name = log_file_name;
  log_file.open(log_file_name, ios::out | ios::app | ios::binary);
  assert(log_file.is_open());
  pthread_mutex_init(&mtx,NULL);

}

void Logger::release() {
  log_file.close(); 
}

LogRecord * Logger::createRecord( 
    uint64_t txn_id,
    LogIUD iud,
    // uint64_t table_id,
    uint64_t key,
    uint64_t part_id
    ) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  record->rcd.init();
  record->rcd.lsn = ATOM_FETCH_ADD(lsn,1);
  record->rcd.iud = iud;
  record->rcd.txn_id = txn_id;
  // record->rcd.table_id = table_id;
  record->rcd.key = key;
  record->rcd.part_id = part_id;
  return record;
}

LogRecord * Logger::createRecord( 
    LogRecord * record
    ) {
  LogRecord * my_record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  my_record->rcd.init();
  my_record->copyRecord(record);
  return my_record;
}


void LogRecord::copyRecord( 
    LogRecord * record
    ) {
  rcd.init();
  rcd.lsn = record->rcd.lsn;
  rcd.iud = record->rcd.iud;
  rcd.type = record->rcd.type;
  rcd.txn_id = record->rcd.txn_id;
  // rcd.table_id = record->rcd.table_id;
  rcd.key = record->rcd.key;
  // log for live migration
  rcd.state = record->rcd.state;
  rcd.part_id = record->rcd.part_id;
  rcd.n_cols = record->rcd.n_cols;
  for (uint32_t i = 0; i < rcd.n_cols; i++) {
    rcd.id_cols[i] = record->rcd.id_cols[i];
  }
  memcpy(rcd.before_image, record->rcd.before_image, 1024);
  memcpy(rcd.after_image, record->rcd.after_image, 1024);
}

LogRecord * Logger::createRecord(
  //LogRecType type,
  uint64_t txn_id,
  LogIUD iud,
  // uint64_t table_id,
  uint64_t key,
  uint32_t state,
  uint32_t part_id,      // partition id
  uint32_t n_cols,
  uint32_t *id_cols,       //id of modified column
  char *before_image,
  char *after_image
) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  record->rcd.init();
  record->rcd.lsn = ATOM_FETCH_ADD(lsn,1);
  record->rcd.iud = iud;
  record->rcd.txn_id = txn_id;
  // record->rcd.table_id = table_id;
  record->rcd.key = key;
  record->rcd.state = state;
  record->rcd.part_id = part_id;
  record->rcd.n_cols = n_cols;
  for (uint32_t i = 0; i < n_cols; i++) {
    record->rcd.id_cols[i] = id_cols[i];
  }
  memcpy(record->rcd.before_image, before_image, 1024);
  memcpy(record->rcd.after_image, after_image, 1024);
  return record;
}


void Logger::enqueueRecord(LogRecord* record) {
  DEBUG("Enqueue Log Record %ld\n",record->rcd.txn_id);
  pthread_mutex_lock(&mtx);
  log_queue.push(record); 
  pthread_mutex_unlock(&mtx);
}

void Logger::processRecord(uint64_t thd_id) {
  if(log_queue.empty())
    return;
  LogRecord * record = NULL;
  pthread_mutex_lock(&mtx);
  if(!log_queue.empty()) {
    record = log_queue.front(); 
    log_queue.pop();
  }
  pthread_mutex_unlock(&mtx);

  if(record) {
    uint64_t starttime = get_sys_clock();
    DEBUG("Dequeue Log Record %ld\n",record->rcd.txn_id);
    if(record->rcd.iud == L_NOTIFY) {
      flushBuffer(thd_id);
      work_queue.enqueue(thd_id,Message::create_message(record->rcd.txn_id,LOG_FLUSHED),false);
    }
    // printf("logger processRecoed\n");
    writeToBuffer(thd_id,record);
    //writeToBuffer((char*)(&record->rcd),sizeof(record->rcd));
    log_buf_cnt++;
    mem_allocator.free(record,sizeof(LogRecord));
    INC_STATS(thd_id,log_process_time,get_sys_clock() - starttime);
  }
  
}

uint64_t Logger::reserveBuffer(uint64_t size) {
  return ATOM_FETCH_ADD(aries_write_offset,size);
}


//void Logger::writeToBuffer(char * data, uint64_t offset, uint64_t size) {
void Logger::writeToBuffer(uint64_t thd_id, char * data, uint64_t size) {
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t starttime = get_sys_clock();
  log_file.write(data,size);
  INC_STATS(thd_id,log_write_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,log_write_cnt,1);

}

void Logger::notify_on_sync(uint64_t txn_id) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  record->rcd.init();
  record->rcd.txn_id = txn_id;
  record->rcd.iud = L_NOTIFY;
  enqueueRecord(record);
}

void Logger::writeToBuffer(uint64_t thd_id, LogRecord * record) {
  DEBUG("Buffer Write\n");
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t starttime = get_sys_clock();
#if LOG_COMMAND

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.txn_id);
  //WRITE_VAL(log_file,record->rcd.partid);
#if WORKLOAD == TPCC
  WRITE_VAL(log_file,record->rcd.txntype);
#endif
  WRITE_VAL_SIZE(log_file,record->rcd.params,record->rcd.params_size);

#else

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.iud);
  WRITE_VAL(log_file,record->rcd.txn_id);
  // WRITE_VAL(log_file,record->rcd.table_id);
  WRITE_VAL(log_file,record->rcd.key);
  // log for live migrarion
  WRITE_VAL(log_file, record->rcd.state);
  WRITE_VAL(log_file, record->rcd.part_id);
  WRITE_VAL(log_file, record->rcd.n_cols);
  for (uint32_t i = 0; i < MAX_NUM_COL; i++) {
    WRITE_VAL(log_file, record->rcd.id_cols[i]);
  }
  WRITE_VAL_SIZE(log_file, record->rcd.before_image, MAX_TUPLE_SIZE);
  WRITE_VAL_SIZE(log_file, record->rcd.after_image, MAX_TUPLE_SIZE);
  /*
  WRITE_VAL(log_file,record->rcd.n_cols);
  WRITE_VAL(log_file,record->rcd.cols);
  WRITE_VAL_SIZE(log_file,record->rcd.before_image,record->rcd.before_image_size);
  WRITE_VAL_SIZE(log_file,record->rcd.after_image,record->rcd.after_image_size);
  */

#endif
  INC_STATS(thd_id,log_write_time,get_sys_clock() - starttime);

}

void Logger::flushBufferCheck(uint64_t thd_id) {
  if(log_buf_cnt >= g_log_buf_max || get_sys_clock() - last_flush > g_log_flush_timeout) {
    flushBuffer(thd_id);
  }
}

void Logger::flushBuffer(uint64_t thd_id) {
  DEBUG("Flush Buffer\n");
  uint64_t starttime = get_sys_clock();
  log_file.flush();
  INC_STATS(thd_id,log_flush_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,log_flush_cnt,1);

  last_flush = get_sys_clock();
  log_buf_cnt = 0;
}
