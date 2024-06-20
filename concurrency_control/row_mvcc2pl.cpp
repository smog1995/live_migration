#include "row_mvcc2pl.h"

class table_t;

void MVCCRow::init(row_t* row) {
    origin_row_ = row;
    list_head_ = NULL;
    list_length_ = 0;
    latch_ = false;
}

RC MVCCRow::writeVersion(TxnManager* txn_man) {
    // cout <<"事务" << txn_man->get_txn_id()<< "插入版本" << endl;
    // printf("事务%ld插入写版本row_key:%ld\n",txn_man->get_txn_id(), origin_row_->get_primary_key());
    MVCCTransNode* new_node = (MVCCTransNode *) mem_allocator.alloc(sizeof(MVCCTransNode));
    row_t* new_row = (row_t*) mem_allocator.alloc(sizeof(row_t));
    
    table_t* table = origin_row_->get_table();
    new_row->init(table, origin_row_->get_part_id(), origin_row_->get_row_id());
    new_row->copy(txn_man->cur_row);
    // new_row->set_primary_key(txn_man->cur_row->get_primary_key());
    // // table->get_new_row(new_row, row->get_part_id(), 0);
    // new_row->tuple_size = txn_man->last_row->get_tuple_size(); // new_row没有初始化表，成员tuple_size需要单独设置下
    // new_row->data = (char *) mem_allocator.alloc(sizeof(char) * new_row->tuple_size);
    // // new_row->copy(txn_man->last_row);
    // new_row->set_data(txn_man->last_row->get_data());
    // // new_row->init(txn_man->cur_row->get_table(), txn_man->cur_row->get_part_id());  版本行不需要存储元数据，只有第一个行需要

    new_node->next = NULL;
    new_node->row = new_row;
    new_node->flag_ = EXEC;
    new_node->txn_id = txn_man->get_txn_id();
    new_node->ts = txn_man->get_timestamp(); // 可能用不上
    // 插入到连头
    if (list_head_) {
        new_node->next = list_head_;
        list_head_ = new_node;
    } else {
        list_head_ = new_node;
    }
    ++list_length_;
    txn_man->cur_row = new_row;
    if (list_length_ >= HIS_RECYCLE_LEN) {
        gabbageCollection(txn_man);
    }
    return RCOK;
}
void MVCCRow::dump() {
    MVCCTransNode* cur_node = list_head_;
    int i = 0;
    // printf("dump:");
    while(cur_node) {
        // printf(" %d:(txnid:%ld)",i++, cur_node->txn_id);
        cur_node = cur_node->next;


    }
    // printf("dump_finish\n");
    glob_manager.lock_manager.lockRequestDump(origin_row_->get_primary_key(), origin_row_->get_table_name());
}
RC MVCCRow::touchWriteVersion(TxnManager* txn_man) {
    MVCCTransNode* cur_node = list_head_;
    // printf("事务%ld对rowkey:%ld触摸写集:\n",txn_man->get_txn_id(),origin_row_->get_primary_key());
    // dump();
    // if (list_head_->txn_id != txn_man->get_txn_id()) {
    //     printf("触摸写集rowkey(%ld)逻辑有误,list_head的txnid:%ld,当前事务id:%ld\n",origin_row_->get_primary_key(),list_head_->txn_id, txn_man->get_txn_id());
    // }
    // assert(list_head_->txn_id == txn_man->get_txn_id());
    while (cur_node) {
        if (cur_node->txn_id == txn_man->get_txn_id()) {
            cur_node->flag_ = FIN;
            // break;
        }
        cur_node = cur_node->next;
    }
    // cur_node->flag_ = FIN;

    return RCOK;
}
//  读已提交,可重复读的实现太麻烦了
RC MVCCRow::readVersion(TxnManager* txn_man, bool snapshot_read) {
// #if ISOLATION_LEVEL == READ_COMMITTED
    RC rc = RCOK;
    // ts_t txn_start_ts = txn_man->get_start_timestamp();
    MVCCTransNode* cur_node = list_head_;
    while (cur_node) {
        if (cur_node->flag_ == FIN) {
            txn_man->cur_row = cur_node->row;
            // printf("事务%ld 对row%ld读快照版本\n ",txn_man->get_txn_id(), txn_man->cur_row->get_primary_key());
            break;
        } else if (cur_node->flag_ == EXEC && cur_node->txn_id == txn_man->get_txn_id()) {
            txn_man->cur_row = cur_node->row;
            break;
        } else if (cur_node->flag_ == EXEC) {//  不做任何操作，仅调试使用
            // printf("事务%ld 对row%ld读到未提交的版本\n ",txn_man->get_txn_id(), txn_man->cur_row->get_primary_key());
        }
        cur_node = cur_node->next;
    }
    if (cur_node == NULL) {  //  说明只能读到原版本
            // printf("事务%ld 对row%ld读到原版本\n",txn_man->get_txn_id(), origin_row_->get_primary_key());
            txn_man->cur_row = origin_row_;
            // if (strlen(origin_row_->get_data()) == 0) {
            //     printf("origin_row_data为空,table为:%s,tuple_size为:%d,primary_key为:%ld\n",origin_row_->get_table_name(), origin_row_->get_tuple_size(),origin_row_->get_primary_key());
            // }
            // assert(strlen(origin_row_->get_data()) != 0);
    }
    return RCOK;
// #endif
}
RC MVCCRow::access(access_t type, TxnManager* txn_man, row_t* & row) {
    RC rc = RCOK;
    if (type == WR) {
        string table_name = origin_row_->get_table_name();
        // printf("access函数:事务%ld尝试对%s的row%ld加锁\n",txn_man->get_txn_id(), table_name.c_str(),origin_row_->get_primary_key());
        rc = glob_manager.lock_manager.lockRow(txn_man, LOCK_EX, origin_row_->get_primary_key(), table_name);
        // manager.add
    }
    //  再次唤醒直接返回
    if (rc == WAIT) {
        return rc;
    }
    readVersion(txn_man); //  会获取一个能读到的版本放入txn_man->cur_row 
    if (type == WR) {// 返回一个读版本
        writeVersion(txn_man);// 写操作会基于cur_row来更新
    }
    row = txn_man->cur_row;
    return rc;
}

RC MVCCRow::rollbackVersion(TxnManager* txn_man) {
    cout << "回滚版本 "; 
    MVCCTransNode* cur_node = list_head_;
    MVCCTransNode* pre_node = NULL;
    while (cur_node) {
        if (cur_node->txn_id == txn_man->get_txn_id()) {
            MVCCTransNode* next_ = cur_node->next;
            if (list_head_ == cur_node) {
                list_head_ = next_;
            }
            assert(cur_node->row);
            // item表的行有空数据
            if (strlen(cur_node->row->data) != 0) {
                mem_allocator.free(cur_node->row->data, cur_node->row->tuple_size); // row的数据缓冲区
            }
            mem_allocator.free(cur_node->row,sizeof(row_t));  // row类对象
            mem_allocator.free(cur_node, sizeof(MVCCTransNode)); //  写版本的数据结构
            cur_node = next_;
            if (pre_node != NULL) {
                pre_node->next = next_;
            }
            --list_length_;
            cout << "回滚成功" << endl;
        } else {
            pre_node = cur_node;
            cur_node = cur_node->next;
        }
    }
    return RCOK;
}

void MVCCRow::gabbageCollection(TxnManager* txn_man) {
    ts_t min_ts = txn_table.get_min_ts(0);
    MVCCTransNode* cur_node = list_head_;
    // MVCCTransNode* pre_node = NULL;
    assert(list_head_ != NULL);
    while (cur_node->next) {
        
        if (cur_node->next->ts < min_ts) {
            MVCCTransNode* x = cur_node->next;
            MVCCTransNode* next = x->next;
            // assert(cur_node->row);

            if (strlen(cur_node->row->get_data()) != 0) {
                mem_allocator.free(x->row->data, x->row->tuple_size); // row的数据缓冲区
            }
            mem_allocator.free(x->row,sizeof(row_t));  // row类对象
            mem_allocator.free(x, sizeof(MVCCTransNode)); //  写版本的数据结构
            cur_node->next = next;
            --list_length_;
        } else {
            cur_node = cur_node->next;
        }
    }
    if (list_length_ == 0) {
        // cout << "链清空为0 ,但是应该不会出现" << endl;
       list_head_ = NULL; 
    }
    // cout << "gc完成,剩下的长度为:" << list_length_ << endl;
    
}