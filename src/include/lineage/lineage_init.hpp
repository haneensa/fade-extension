#pragma once
#include "duckdb.hpp"

namespace duckdb {

struct AggInfo {
  int child_agg_id;
  bool has_agg_child;
  idx_t n_groups_attr;
  int count_idx = -1;
	std::unordered_map<int, void*> input_data_map; // input to the aggregates
	std::unordered_map<int, LogicalType> alloc_vars_types; // attr type per output
	std::unordered_map<int, string> alloc_vars_funcs; // ith aggregate function name
	std::unordered_map<int, idx_t> alloc_vars_col_idx; // ith aggregate function name
  vector<std::pair<vector<Vector>, int>> cached_cols;
};

// tree of lineage points (ignore pipelined operators because we pipeline their lineage)
struct LineageInfoNode {
  idx_t opid;
  LogicalOperatorType type;
  vector<idx_t> children;
  int n_output;
  int n_input;
  unique_ptr<AggInfo> agg_info;
  LineageInfoNode(idx_t opid, LogicalOperatorType type) : opid(opid), type(type),
  n_output(0), n_input(-1) {}
};

struct LineageState {
   static idx_t query_id;
   static idx_t global_id;
   static bool capture;
   static bool persist;
   static bool debug;
   static idx_t table_idx;
   static std::unordered_map<string, LogicalOperatorType> lineage_types;
   static std::unordered_map<string, vector<std::pair<Vector, int>>> lineage_store;
   static std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> qid_plans;
   static std::unordered_map<idx_t, idx_t> qid_plans_roots;
};



unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                      unique_ptr<LogicalOperator>& plan);

} // namespace duckdb
