#include "txn.h"
#include "row.h"
#include "manager.h"
#include "mem_alloc.h"
#include "global.h"

#ifndef ROW_MVCC2PL_H
#define ROW_MVCC2PL_H

class table_t;
class Catalog;
class TxnManager;
struct MVCCTransNode {	
	// 版本只维护了行的数据，而不维护行的其他信息，其他信息我们在一开始从index中的初始行获取，持久化到index时再覆盖原行的数据，就不用多保存其他数据
	ts_t ts;
	uint64_t lsn;  // 数据所在的日志
	// uint64_t seq_no_;  // 事务中的sql number,可能一个存储过程有多个相同sql no 的行版本
	row_t * row;
	txnid_t txn_id;
	TxnState  flag_;  //事务状态（仅使用未提交0、已提交1、终止2） 
	MVCCTransNode * next = NULL;
};

class MVCCRow {
public:
	MVCCRow(){};
	void init(row_t* row);
	// RC readVersion(ts_t ts, row_t* &row, TxnManager* txn_man, bool snapshot_read = true);  //这里的row并无实际作用，我们直接让txn的cur_row来保存读版本
	void dump();
	RC readVersion(TxnManager* txn_man, bool snapshot_read = true);
	RC touchWriteVersion(TxnManager* txn_man);
	RC writeVersion(TxnManager* txn_man);
	RC rollbackVersion(TxnManager* txn_man);
	RC access(access_t type, TxnManager* txn_man, row_t* & row);
	void gabbageCollection(TxnManager* txn_man);
private:
	row_t* origin_row_;
	MVCCTransNode* list_head_ = NULL;
	bool latch_;
	uint32_t list_length_ = 0;
};
#endif