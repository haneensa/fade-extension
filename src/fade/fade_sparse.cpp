// DONE: cache agg input values and output values
// TODO: use cached agg input values, and lineage.
// TODO: end to end use case using the demo
// TODO: get table name and binding info
// TODO: read base annotations : check src/optimizer/compressed_materialization.cpp
// TODO create pipeline: leaf nodes are scan nodes for any table we want to intervene on.
//                       add compressing projection, read only the attributes we care about. 
//                       then parent is the fade node that takes this as input and start the internvention
#include <iostream>

#include "fade_extension.hpp"
#include "fade/fade_node.hpp"
#include "lineage/lineage_init.hpp"

namespace duckdb {

// lineage: 1D backward lineage
// var: sparse input intervention matrix
// out: sparse output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
int whatif_sparse_filter(vector<int>& lineage,
                         int* __restrict__ var,
                         int* __restrict__ out,
                         int start, int end) {
	for (int i=start; i < end; ++i) {
		out[i] = var[lineage[i]];
	}
	return 0;
}

// lhs/rhs_lineage: 1D backward lineage
// lhs/rhs_var: sparse input intervention matrix
// out: sparse output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
// left/right_n_interventions: how many annotations the left/right side has
int whatif_sparse_join(int* lhs_lineage, int* rhs_lineage,
                       int* __restrict__ lhs_var,  int* __restrict__ rhs_var,
                       int* __restrict__ out, const int start, const int end,
                       int left_n_interventions, int right_n_interventions) {
	if (left_n_interventions > 1 && right_n_interventions > 1) {
		for (int i=start; i < end; i++) {
			out[i] = lhs_var[lhs_lineage[i]] * right_n_interventions + rhs_var[rhs_lineage[i]];
		}
	} else if (left_n_interventions > 1) {
		for (int i=start; i < end; i++) {
			out[i] = lhs_var[lhs_lineage[i]];
		}
	} else {
		for (int i=start; i < end; i++) {
			out[i] = rhs_var[rhs_lineage[i]];
		}
	}

	return 0;
}

void InterventionSparse(int query_id, idx_t operator_id, idx_t thread_id, std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data) {
  auto &lop_info = LineageState::qid_plans[query_id][operator_id];
  auto &fnode = fade_data[operator_id];
  std::cout << "InterventionSparse " << lop_info->opid << " " << lop_info->children.size() << std::endl;
  string table_name = to_string(query_id) + "_" + to_string(lop_info->opid);
  for (auto &child : lop_info->children) {
    InterventionSparse(query_id, child, thread_id, fade_data);
  }

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
    std::cout << "logical get. TODO: access base annotations" << std::endl;
    if (fnode->n_interventions <= 1) return;
      // ?
    break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
    std::cout << "logical filter. TODO: access lineage " << std::endl;
    if (FadeState::prune) return;
    if (fnode->n_interventions <= 1) return;
    // auto lineage = LineageState::lineage_store_post[query_id][operator_id]; // backward lineage
    vector<int> lineage;
    idx_t lineage_size = 0;
		pair<int, int> start_end_pair = get_start_end(lineage_size, thread_id, FadeState::num_worker);
    auto base_annotations = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[0]].get())->annotations.get();
    auto annotations = dynamic_cast<FadeSparseNode*>(fnode.get())->annotations.get();
    whatif_sparse_filter(lineage, base_annotations, annotations, start_end_pair.first, start_end_pair.second);
    // use cur_node->mtx to notify workers we are done
     break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
     int lhs_n = LineageState::lineage_store[table_name].size();
     int rhs_n = LineageState::lineage_store[table_name+"_right"].size();
     std::cout << "logical join. TODO: access lineage, its size,  " << lhs_n << " " << rhs_n << " " << lop_info->n_output << std::endl;
     if (fnode->n_interventions <= 1) return;
     int lhs_n_interventions = fade_data[lop_info->children[0]]->n_interventions;
     int rhs_n_interventions = fade_data[lop_info->children[1]]->n_interventions;
     int* lhs_lineage; int* rhs_lineage;
     idx_t lineage_size = 0;
     auto lhs_annotations = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[0]].get())->annotations.get();
     auto rhs_annotations = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[0]].get())->annotations.get();
     auto annotations = dynamic_cast<FadeSparseNode*>(fnode.get())->annotations.get();
     pair<int, int> start_end_pair = get_start_end(lineage_size, thread_id, FadeState::num_worker);
     whatif_sparse_join(lhs_lineage, rhs_lineage, lhs_annotations, rhs_annotations, annotations,
          start_end_pair.first, start_end_pair.second, lhs_n_interventions, rhs_n_interventions);
     // use cur_node->mtx to notify workers we are done
     break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    int n = LineageState::lineage_store[table_name].size();
    std::cout << "logical agg. TODO: get lineage " << n << " " << lop_info->n_output <<std::endl;
    // if no interventions or number of threads per node is less than global number
    if (fnode->n_interventions <= 1 || thread_id >= fnode->num_worker) return;
    idx_t n_input = 0; //chunk_collection.Count(); // cached data
		pair<int, int> start_end_pair = get_start_end(n_input, thread_id, fnode->num_worker);
    // if number of groups requested is empty, use forward lineage
    // DEFAULT: use backward since this is the rep we get
    int* annotations_ptr = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[0]].get())->annotations.get();
    // int* forward_lineage_ptr = op->lineage_op->forward_lineage[0].data();
    for (auto& out_var : lop_info->agg_info->alloc_vars_funcs) {
      int col_idx = out_var.first;
      string func = out_var.second;
      LogicalType &typ = lop_info->agg_info->alloc_vars_types[col_idx];
      //    if (func == "count") { 
      //      int groups_count = ?;
      //    } else if (func == "avg" || func == "stddev" || func == "sum") {
      //      int groups_sum;
      //      if (func == "stddev") {
      //        int groups_sum_2;
      //      }
      //    }
    }
    /*
    //  for (int g=0; g < groups.size(); ++g) {
    //    int gid = groups[g];
    //    // TODO: do this once!
    //    std::vector<int>& bw_lineage = lineage_store[][gid];
    //    if (func == "count") {
    //      int groups_count = bw_lineage.size;
    //    } else if (func == "avg" || func == "stddev" || func == "sum") {
    //      float *in_arr = reinterpret_cast<float *>(cur_node->input_data_map[col_idx]);
    //      for (int i =0; i < bw_lineage.size(); ++i) {
    //        int iid = bw_lineage[i];
    //        groups_sum[g] += in_arr[iid];
    //      }
    //
    //      if (func == "stddev") {
    //        float *in_arr = reinterpret_cast<float *>(cur_node->input_data_map[col_idx]);
    //        for (int i=0; i < bw_lineage.size(); ++i) {
    //          int iid = bw_lineage[i];
    //          groups_sum_2[g] += (in_arr[iid] * in_arr[iid]);
    //        }
    //      }
    //    }
          groupby_agg_incremental_arr_single_group_bw(g, bw_lineage, annotations_ptr,
                                      cur_node->alloc_vars[out_var.first][thread_id],
                                      cur_node->input_data_map,
                                      cur_node->n_interventions,
                                      col_idx, func, typ);
    //    
    //    
    //  }
    // }
    // groupby_agg_incremental_arr();
    // groupby_agg_incremental_arr_single_group_bw();
    */
     break;
   } default: {}}

}

void WhatIfSparse(int qid, int aggid, string spec, vector<int> groups) {
  if (LineageState::qid_plans_roots.find(qid) == LineageState::qid_plans_roots.end()) return;
  // 1. Parse spec. Input (t.col1|t.col2|..)
  std::unordered_map<std::string, std::vector<std::string>> columns_spec = parseSpec(spec);
  // 2. traverse query plan, allocate fade nodes, and any memory allocation, read input?
  std::unordered_map<idx_t, unique_ptr<FadeNode>> fade_data;
  PrepareFadePlan(qid, LineageState::qid_plans_roots[qid], fade_data);
  // 4. GetCachedData(); ?
  // 5. Evaluate Interventions
  // 5.1 TODO: use workers
  InterventionSparse(qid, LineageState::qid_plans_roots[qid], 0, fade_data);
  // 6. store output in global storage to be accessed later by the user
}

} // namespace duckdb
