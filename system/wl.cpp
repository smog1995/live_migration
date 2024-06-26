/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "migration_index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "mem_alloc.h"

RC Workload::init() {
	return RCOK;
}

RC Workload::init_schema(const char * schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);	
	string line;
  uint32_t id = 0;
	ifstream fin(schema_file);
    Catalog * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname(&line[6]);
			void * tmp = new char[CL_SIZE * 2 + sizeof(Catalog)];
      schema = (Catalog *) ((UInt64)tmp + CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( tname.c_str(), id++, lines.size() );
			for (UInt32 i = 0; i < lines.size(); i++) {
				string line = lines[i];
				vector<string> items;

        char * line_cstr = new char [line.length()+1];
        strcpy(line_cstr,line.c_str());
        int size = atoi(strtok(line_cstr,","));
        char * type = strtok(NULL,",");
        char * name = strtok(NULL,",");

        schema->add_col(name, size, type);
				col_count ++;
			} 
			tmp = new char[CL_SIZE * 2 + sizeof(table_t)];
            table_t * cur_tab = (table_t *) ((UInt64)tmp + CL_SIZE);
			cur_tab->init(schema);
			tables[tname] = cur_tab;
        } else if (!line.compare(0, 6, "INDEX=")) {
			string iname(&line[6]);
			getline(fin, line);

			vector<string> items;
			string token;
			size_t pos;
			while (line.length() != 0) {
				pos = line.find(","); // != std::string::npos) {
				if (pos == string::npos)
					pos = line.length();
	    		token = line.substr(0, pos);
				items.push_back(token);
		    	line.erase(0, pos + 1);
			}
			
			string tname(items[0]);
      size_t tname_len = 0;
			int field_cnt = items.size() - 1;
			uint64_t * fields = new uint64_t [field_cnt];
			for (int i = 0; i < field_cnt; i++) 
				fields[i] = atoi(items[i + 1].c_str());
      INDEX * index = new INDEX;
	    int part_cnt __attribute__ ((unused));
			part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;

      uint64_t table_size = g_synth_table_size;
#if WORKLOAD == TPCC
      // 表的数量（指表的分区数量）
      if ( !tname.compare(1, 9, "WAREHOUSE") ) {
        tname_len = 9;
        table_size = g_num_wh / g_part_cnt;
        printf("WAREHOUSE size %ld\n",table_size);
      } else if ( !tname.compare(1, 8, "DISTRICT") ) {
        tname_len = 8;
        table_size = g_num_wh / g_part_cnt * g_dist_per_wh;
        printf("DISTRICT size %ld\n",table_size);
      } else if ( !tname.compare(1, 8, "CUSTOMER") ) {
        tname_len = 8;
        table_size = g_num_wh / g_part_cnt * g_dist_per_wh * g_cust_per_dist;
        printf("CUSTOMER size %ld\n",table_size);
      } else if ( !tname.compare(1, 7, "HISTORY") ) {
        tname_len = 7;
        table_size = g_num_wh / g_part_cnt * g_dist_per_wh * g_cust_per_dist;
        printf("HISTORY size %ld\n",table_size);
      } else if ( !tname.compare(1, 5, "ORDER") ) {
        tname_len = 5;
        table_size = g_num_wh / g_part_cnt * g_dist_per_wh * g_cust_per_dist;
        printf("ORDER size %ld\n",table_size);
      } else if ( !tname.compare(1, 4, "ITEM") ) {
        tname_len = 4;
        table_size = g_max_items;
        printf("ITEM size %ld\n",table_size);
      } else if ( !tname.compare(1, 5, "STOCK") ) {
        tname_len = 5;
        table_size = g_num_wh / g_part_cnt * g_max_items;
        printf("STOCK size %ld\n",table_size);
      }
#elif WORKLOAD == PPS
      if ( !tname.compare(1, 5, "PARTS") ) {
        tname_len = 5;
        table_size = MAX_PPS_PART_KEY;
      }
      else if ( !tname.compare(1, 8, "PRODUCTS") ) {
        tname_len = 8;
        table_size = MAX_PPS_PRODUCT_KEY;
      }
      else if ( !tname.compare(1, 9, "SUPPLIERS") ) {
        tname_len = 9;
        table_size = MAX_PPS_SUPPLIER_KEY;
      }
      else if ( !tname.compare(1, 8, "SUPPLIES") ) {
        tname_len = 8;
        table_size = MAX_PPS_PRODUCT_KEY;
      }
      else if ( !tname.compare(1, 4, "USES") ) {
        tname_len = 4;
        table_size = MAX_PPS_SUPPLIER_KEY;
      }
#else
      table_size = g_synth_table_size / g_part_cnt;
#endif
#if INDEX_STRUCT == IDX_HASH
			index->init(1024, tables[tname.substr(1,tname_len)], table_size);
			//index->init(part_cnt*1024, tables[tname], table_size);
#elif INDEX_STRUCT == IDX_MIGRATION_HASH      
      index->init(1024, tables[tname.substr(1,tname_len)], table_size);
#else
			index->init(part_cnt, tables[tname.substr(1,tname_len)]);
#endif
			indexes[iname] = index;
		}
    // cout << "打印表的schema" ;
    // schema->print_schema();
    }
	fin.close();
  
	return RCOK;
}


void Workload::index_delete_all() {
  /*
  for (auto string index_name = indexes.keys(); index_name = index_name.next()) {
    INDEX * index = (INDEX *) indexes[index_name];
    index->index_delete();
  }
  */
}

void Workload::index_insert(string index_name, uint64_t key, row_t * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row);
}

void Workload::index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t));
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

  assert(index);
  assert( index->index_insert(key, m_item, pid) == RCOK );
}

void Workload::index_insert_nonunique(INDEX * index, uint64_t key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t));
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

  assert(index);
  assert( index->index_insert_nonunique(key, m_item, pid) == RCOK );
}



