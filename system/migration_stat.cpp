#include "migration_stat.h"


void MigrationStat::addBlockedTxn(txnid_t txn_id, bool remote_txn, std::chrono::system_clock::time_point start_block_time) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	if (blocked_txns.find(txn_id) == blocked_txns.end()) {
		blocked_txns.insert({txn_id, unique_ptr<TxnEntry>(new TxnEntry(txn_id, remote_txn,start_block_time))});
	} else {
		blocked_txns[txn_id]->start_block_time_ = start_block_time;
	}

	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
}
void MigrationStat::setUnblockedTxn(txnid_t txn_id) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	if (blocked_txns.find(txn_id) != blocked_txns.end()) {
		blocked_txns[txn_id]->blocked = false;
	}
	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
}
void MigrationStat::removeTxn(txnid_t txn_id) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	if (blocked_txns.find(txn_id) != blocked_txns.end()) {
		blocked_txns.erase(txn_id);
	}
	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
}

void MigrationStat::calculateBlockTime(uint64_t thd_id) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	// auto cur_time = 
	std::chrono::system_clock::time_point cur_time = std::chrono::system_clock::now();
	int block_overtime_txn_cnt = 0;
	vector<txnid_t> overtime_txns;
	for (auto &[txn_id, txn_entry] : blocked_txns) {
		if (txn_entry->blocked) {
			txn_entry->blocked_time_ = std::chrono::duration_cast<std::chrono::duration<double>>(cur_time - txn_entry->start_block_time_);
			if (txn_entry->blocked_time_ > limit_block_overtime) {
				++block_overtime_txn_cnt;
				overtime_txns.push_back(txn_id);
			}
		}
	}
	for (auto txnid : overtime_txns) {
		blocked_txns.erase(txnid);
	}
	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
	abortOvertimeTxn(thd_id, overtime_txns);
}
void MigrationStat::abortOvertimeTxn(uint64_t thd_id, vector<txnid_t> &overtime_txns) {
	for (size_t i = 0; i < overtime_txns.size(); i++) {
		txn_table.restart_txn_abort(thd_id, overtime_txns[i]);
	}
}

void MigrationStat::caculateIntervalThroughput() {
	
	interval_throughput[0].push_back(local_abort_txn - current_throughput[0]);
	interval_throughput[1].push_back(local_commit_txn - current_throughput[1]);
	interval_throughput[2].push_back(remote_commit_txn - current_throughput[2]);
	interval_throughput[3].push_back(remote_abort_txn - current_throughput[3]);
	interval_throughput[4].push_back(imitate_abort_txn - current_throughput[4]);
	interval_throughput[5].push_back(imitate_commit_txn - current_throughput[5]);

	current_throughput[0] = local_abort_txn;
	current_throughput[1] = local_commit_txn;
	current_throughput[2] = remote_commit_txn;
    current_throughput[3] = remote_abort_txn;
    current_throughput[4] = imitate_abort_txn;
	current_throughput[5] = imitate_commit_txn;
}

void MigrationStat::printStats() {

	printf("事务统计:本节点事务开始数量:%d\n",start_txn);
	printf("本地分区事务数量:%d,提交数量:%d,终止数量:%d\n",local_txn, local_commit_txn, local_abort_txn);
	printf("本地发起的远程事务数量:%d,提交数量:%d,终止数量:%d\n",remote_txn, remote_commit_txn, remote_abort_txn);
	printf("本地发起的模仿事务数量:%d,提交数量:%d,终止数量:%d\n",imitate_txn,imitate_commit_txn,imitate_abort_txn);
	for (int i = 0; i < throughput_stats_size; i++) {
		for (int j = 0; j < interval_throughput[i].size();j++) {
			printf("%d ", interval_throughput[i][j]);
		}
		printf("\n");
	}
}