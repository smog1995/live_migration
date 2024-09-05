#include "global.h"
#include <unordered_map>

struct TxnEntry {
	txnid_t txn_id_;
	std::chrono::system_clock::time_point start_block_time_; //  每次被阻塞，都要重新设置
	bool remote_txn_;
	std::chrono::duration<double> blocked_time_;
	bool blocked; // 起初为true
	TxnEntry(txnid_t txn_id, bool remote_txn,std::chrono::system_clock::time_point start_block_time):txn_id_(txn_id),remote_txn_(remote_txn),start_block_time_(start_block_time){
		remote_txn_ = false;
		blocked = true;
	}
};

class MigrationStat {
public:
    MigrationStat() {
        limit_block_overtime = std::chrono::seconds(1);
		start_txn = 0;
		remote_txn = 0;
		imitate_txn = 0;
        local_abort_txn = 0;
        remote_abort_txn = 0;
        imitate_abort_txn = 0;
		local_commit_txn = 0;
		remote_commit_txn = 0;
		imitate_commit_txn = 0;
		throughput_stats_size = 6;
		for (int i = 0; i < throughput_stats_size; i++) {
			current_throughput.push_back(0);
			interval_throughput.emplace_back(vector<int>());
		}
	// cout << "duration为一秒" << limit_block_overtime.count() << endl;
    }


    //  事务阻塞超时处理
	void 		addBlockedTxn(txnid_t txn_id, bool remote_txn,std::chrono::system_clock::time_point start_block_time);
	void 		calculateBlockTime(uint64_t thd_id);
	void 		removeTxn(txnid_t txn_id);
	void 		setUnblockedTxn(txnid_t txn_id);
	void		abortOvertimeTxn(uint64_t thd_id, vector<txnid_t> &overtime_txns);

	void caculateIntervalThroughput();
	void		printStats();
    int start_txn; //   remote_txn + local_txn + imitate_txn = start_txn   
	int local_txn;
	int remote_txn; 
	int imitate_txn; 
	// 事务中止统计

	int throughput_stats_size;
    int local_abort_txn;
	int local_commit_txn;
	int remote_commit_txn;
    int remote_abort_txn;
    int imitate_abort_txn;
	int imitate_commit_txn;
	

	vector<int> current_throughput;
//  interval为5s的throughput
	vector<vector<int>> interval_throughput;
private:
    	//  事务阻塞超时处理
	unordered_map<txnid_t, unique_ptr<TxnEntry>> blocked_txns;
	std::chrono::duration<double>	limit_block_overtime; // <double, std::seconds>应该默认是秒
	bool blocked_txns_map_latch;

	
    
};