#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_mvcc.h"
#include "mem_alloc.h"

class table_t;
class Catalog;
class TxnManager;

struct MVReqEntry {
	TxnManager * txn;
	ts_t ts;
	ts_t starttime;
	MVReqEntry * next;
};

struct MVHisEntry {	
	ts_t ts;
	// only for write history. The value needs to be stored.
//	char * data;
	row_t * row;
	MVHisEntry * next;
	MVHisEntry * prev;
};