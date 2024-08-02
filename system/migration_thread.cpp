#include "global.h"
#include "thread.h"
#include "migration_thread.h"
#include "client_thread.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "message.h"
#include "wl.h"
class Workload;
void MigrationThread::setup() {
    cout << "migrationThread setup" << endl;
    live_migration_stage = SNAPSHOT_TRANS;
    cur_table_index = 0;
    schema_file = "benchmarks/TPCC_full_schema.txt";
    finished = false;
    cout << schema_file << "路径";
    init_migration_table_list();
}

void MigrationThread::init_migration_table_list() {
    ifstream fin(schema_file);
    string line;
    // assert(fin.is_open());
     if (!fin.is_open()) {
        cerr << "Error: Could not open file " << schema_file << endl;
        cerr << "Reason: " << strerror(errno) << endl;
    }
    while (getline(fin, line)) {
      // cout << line << " line";
		if (line.compare(0, 6, "INDEX=") == 0) {
            table_indexs_name.emplace_back(string(&line[6]));
            ++table_count;
            cout << table_indexs_name[table_count - 1] << "tableindex_name" << endl;
        }
    }
    cout << "init_migration_table_list finish" << endl;
}

void MigrationThread::change_stage() {
  switch(live_migration_stage) {
    case SNAPSHOT_TRANS :
      ++cur_table_index;
      if (cur_table_index == table_indexs_name.size()) {
        receive_time = std::chrono::system_clock::now();
        stage_time_cost[live_migration_stage] = std::chrono::duration<double, std::milli>(receive_time - start_time);
        printf("传输快照阶段用时:%lf (ms)\n",stage_time_cost[live_migration_stage].count());
        live_migration_stage = ASYNC_LOGS;
        start_time = receive_time;
        printf("进入二阶段，异步日志传输\n");

      }
      break;
    case ASYNC_LOGS :
      receive_time = std::chrono::system_clock::now();
      stage_time_cost[live_migration_stage] = std::chrono::duration<double, std::milli>(receive_time - start_time);
      printf("异步日志传输阶段用时:%lf (ms)\n",stage_time_cost[live_migration_stage].count());
      live_migration_stage = SYNC_EXEC;
      start_time = receive_time;
      printf("进入三阶段,同步执行阶段\n");
      break;
    case SYNC_EXEC :
      receive_time = std::chrono::system_clock::now();
      stage_time_cost[live_migration_stage] = std::chrono::duration<double, std::milli>(receive_time - start_time);
      printf("同步执行阶段用时:%lf (ms)\n",stage_time_cost[live_migration_stage].count());
      start_time = receive_time;
      
      

      break;
    default :
      assert(false);
      break;
  }
}

RC MigrationThread::run() {
  tsetup();
  printf("Running LiveMigrationThread \n");
  int index = 0;
  LiveMigrationMessage* snap_msg = (LiveMigrationMessage*)Message::create_message(MIGRATION_MSG); // 会在msg线程进行释放
  snap_msg->migration_dest_id = 1; 
  snap_msg->part_id = 0;
  snap_msg->finish = false;
  snap_msg->live_migration_stage = SNAPSHOT_TRANS;
  memcpy(snap_msg->table_index_name, table_indexs_name[cur_table_index].c_str(), table_indexs_name[cur_table_index].size());
  msg_queue.enqueue(get_thd_id(), snap_msg, 0); // 发送到服务器0
  printf("开始进行迁移\n");
 
  printf("快照传输阶段\n");
  printf("开始传输表index%s\n",table_indexs_name[cur_table_index].c_str());
  
  start_time = std::chrono::system_clock::now();
  while(!finished) {
    Message* msg = work_queue.migration_dequeue();
    if(!msg) {
      continue;
    }
    assert(msg->get_rtype() == MIGRATION_ACK);
    change_stage();
    // LiveMigrationAckMessage *migration_ack = (LiveMigrationAckMessage*) msg;  
    //  TODO : 后续可能需要处理ack，进行失败重传之类
    
    if (live_migration_stage == SNAPSHOT_TRANS) {
      snap_msg = (LiveMigrationMessage*)Message::create_message(MIGRATION_MSG); // 会在msg线程进行释放
      snap_msg->migration_dest_id = 1; 
      snap_msg->part_id = 0;
      snap_msg->finish = false;
      snap_msg->live_migration_stage = SNAPSHOT_TRANS;
      memcpy(snap_msg->table_index_name, table_indexs_name[cur_table_index].c_str(), table_indexs_name[cur_table_index].size());
      msg_queue.enqueue(get_thd_id(), snap_msg, 0);
      printf("开始传输表index%s\n",table_indexs_name[cur_table_index].c_str());
    } else if (live_migration_stage == ASYNC_LOGS) {
      start_time = std::chrono::system_clock::now();
      //  目前这里还没实现，我们要测试同步阶段，先实现同步阶段
      LiveMigrationMessage* sync_msg = (LiveMigrationMessage*)Message::create_message(MIGRATION_MSG);
      sync_msg->migration_dest_id = 1;
      sync_msg->part_id = 0;
      sync_msg->finish = false;
      sync_msg->live_migration_stage = ASYNC_LOGS;
      msg_queue.enqueue(get_thd_id(), sync_msg, 0);
      
      //先在这里进行切换路由，后面再改
      usleep(1000000);
      route_exchange = true;
      migra_part_id = 0;
      migra_node_id = 1;
      LiveMigrationMessage *sync_finish_msg = (LiveMigrationMessage*)Message::create_message(MIGRATION_MSG);
      sync_finish_msg->migration_dest_id = 1;
      sync_finish_msg->part_id = 0;
      sync_finish_msg->finish = true;
      sync_finish_msg->live_migration_stage = ASYNC_LOGS;
      printf("同步阶段完成，切换路由导向，新事务往目标节点执行finish\n");
      msg_queue.enqueue(get_thd_id(), sync_finish_msg, 0);
    }
  }

  fflush(stdout);
	return FINISH;
}
