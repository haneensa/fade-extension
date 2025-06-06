#pragma once

#include "fade_extension.hpp"

namespace duckdb {

enum InterventionType {
	DENSE_DELETE,
	SCALE_UNIFORM,
	SCALE_RANDOM,
	SEARCH
};

struct EvalConfig {
	static int batch;
	static int mask_size;
	static bool is_scalar;
	static bool use_duckdb;
	static bool debug;
	static bool prune;
	static bool incremental;
	static int num_worker;
	static bool use_gb_backward_lineage;
	static bool use_preprep_tm;
	static string columns_spec_str;
	static InterventionType intervention_type;
};


class FadeNode {

public:
  bool debug;
  int opid;
  int aggid;
  int n_interventions;
  int num_worker;

  int counter;

	virtual ~FadeNode() {}
  // holds post interventions output. n_output X n_interventions per worker
	std::unordered_map<idx_t, vector<void*>> alloc_vars;
};


class FadeSparseNode : public FadeNode {
public:
/*	FadeSparseNode(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeNode(opid, n_interventions, num_worker, rows, debug) {};
*/
	virtual ~FadeSparseNode() {}
public:
	unique_ptr<int[]> annotations;
};

/*
class FadeNodeSingle: public FadeNode {
public:
	FadeNodeSingle(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeNode(opid, n_interventions, num_worker, rows, debug) {};

	virtual ~FadeNodeSingle() = default; // Virtual destructor

public:
	int8_t* single_del_interventions;
	int8_t* base_single_del_interventions;
};
*/

void PrepareFadePlan(idx_t qid, idx_t opid, std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data);
pair<int, int> get_start_end(int row_count, int thread_id, int num_worker);
std::unordered_map<std::string, std::vector<std::string>>  parseSpec(string& columns_spec_str);

} // namespace duckdb
