#include "fade/fade_node.hpp"

#include <iostream>
#include <fstream>

#include "fade_extension.hpp"
#include "lineage/lineage_init.hpp"

namespace duckdb {

bool FadeState::prune;
idx_t FadeState::num_worker;

pair<int, int> get_start_end(int row_count, int thread_id, int num_worker) {
	int batch_size = row_count / num_worker;
	if (row_count % num_worker > 0) batch_size++;
	int start = thread_id * batch_size;
	int end   = start + batch_size;
	if (end >= row_count)  end = row_count;
	return std::make_pair(start, end);
}

template<class T>
void allocate_agg_output(FadeNode* fnode, int t, int n_interventions,
    int n_output, int out_var) {
	fnode->alloc_vars[out_var][t] = aligned_alloc(64, sizeof(T) * n_output * n_interventions);
	if (fnode->alloc_vars[out_var][t] == nullptr) {
		fnode->alloc_vars[out_var][t] = malloc(sizeof(T) * n_output * n_interventions);
	}
	memset(fnode->alloc_vars[out_var][t], 0, sizeof(T) * n_output * n_interventions);
}

void PrepareFadePlan(idx_t qid, idx_t opid, std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data) { 
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   PrepareFadePlan(qid, child, fade_data);
  }

	unique_ptr<FadeSparseNode> node = make_uniq<FadeSparseNode>();
  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
    std::cout << "logical get. TODO: access base annotations" << std::endl;
    // get table name. specs if any. get size of input. number of unique values for
    // the predicate annotations
    // base_annotations.reset(new int[n_input]);
		// use random initially:
    // Fade::random_unique(node->n_groups, node->base_annotations.get(), n_intervention);
    node->n_interventions = 0;
    break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
    std::cout << "logical filter. TODO: access lineage" << std::endl;
    node->n_interventions = fade_data[lop_info->children[0]]->n_interventions;
    node->annotations.reset(new int[lop_info->n_output]);
    break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
    idx_t lhs_n = fade_data[lop_info->children[0]]->n_interventions;
    idx_t rhs_n = fade_data[lop_info->children[1]]->n_interventions;
    node->n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
    if (node->n_interventions > 1) node->annotations.reset(new int[lop_info->n_output]);
    break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    node->n_interventions = fade_data[lop_info->children[0]]->n_interventions;
    int n_output = lop_info->n_output;
    int n_input = lop_info->n_input;
    std::cout << "logical agg. TODO: get lineage " <<lop_info->agg_info->cached_cols.size() << " " << 
      node->n_interventions << " " << n_input << " " << n_output << std::endl;
    // alloc per worker t. get n_groups from lop_info, n_interventions? taken from child
    for (auto& out_var : lop_info->agg_info->alloc_vars_funcs) {
      int col_idx = out_var.first;
      for (int t=0; t < node->num_worker; ++t) {
        auto &typ = lop_info->agg_info->alloc_vars_types[col_idx];
        if (typ == LogicalType::INTEGER || typ == LogicalType::BIGINT)
          allocate_agg_output<int>(node.get(), t, node->n_interventions, n_output, col_idx);
        else
          allocate_agg_output<float>(node.get(), t, node->n_interventions, n_output, col_idx);
      }
    }

     break;
   } default: {}}
    
   fade_data[opid] = std::move(node);
}

// table_name.col_name|...|table_name.col_name
std::unordered_map<std::string, std::vector<std::string>>  parseSpec(string& columns_spec_str) {
	std::unordered_map<std::string, std::vector<std::string>> result;
  if (columns_spec_str.empty()) return result;

	std::istringstream iss(columns_spec_str);
	std::string token;

	while (std::getline(iss, token, '|')) {
		std::istringstream tokenStream(token);
		std::string table, column;
		if (std::getline(tokenStream, table, '.')) {
			if (std::getline(tokenStream, column)) {
				// Convert column name to uppercase (optional)
				//for (char& c : column) {
				//	c = std::tolower(c);
				//}
				// Add the table name and column to the dictionary
				result[table].push_back(column);
			}
		}
	}

	return result;
}


} // namespace duckdb
