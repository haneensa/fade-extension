#define DUCKDB_EXTENSION_MAIN
#include "fade_extension.hpp"

#include <iostream>

#include "fade/fade_reader.hpp"
#include "lineage/lineage_init.hpp"
#include "lineage/lineage_meta.hpp"
#include "lineage/lineage_reader.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

std::string FadeExtension::Name() {
    return "fade";
}

inline void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::lineage_store.clear();
  LineageState::lineage_types.clear();
}

inline void PragmaEnablePersistLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::persist = true;
}

inline void PragmaDisablePersistLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::persist = false;
}


inline void PragmaEnableLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture = true;
}

inline void PragmaDisableLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture = false;
}

// 1) prepapre lineage: query id, prune, forward/backward
inline void PragmaPrepareLineage(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  // bool prune = parameters.values[1].GetValue<bool>();
  // bool forward_lineage = parameters.values[2].GeteValue<bool>();
  // bool use_gb_backward_lineage = parameters.values[3].GetValue<bool>();
  std::cout << "pragma prepapre lineage " <<  qid << " " << LineageState::capture << " " << LineageState::query_id << " " << LineageState::persist << std::endl;
  // 1) for each pipeline -> (op_id, op_type)
  // table_name = "LINEAGE_" + to_string(query_id) + "_" + to_string(op.operator_id);
  // 2) lineage_store[table_name]
}

// 2) WhatIf: calls fade::whatif API
inline void PragmaFade(ClientContext &context, const FunctionParameters &parameters) {
  std::cout << "pragma whatif: ";
  int qid = parameters.values[0].GetValue<int>();
  string spec = parameters.values[1].ToString();
  string agg_alias = parameters.values[2].ToString();
  auto list_values = ListValue::GetChildren(parameters.values[3]);
  std::cout << qid << " " << spec << " " << agg_alias << " " << list_values.size() << std::endl;
  vector<int> groups;
  for (idx_t i = 0; i < list_values.size(); ++i) {
      auto &child = list_values[i];
      groups.push_back(child.GetValue<int>());
  }
  // // iterate over final aggregates column names to get the index for aggid
  int aggid = 0;
  WhatIfSparse(qid, aggid, spec, groups);
}


void FadeExtension::Load(DuckDB &db) {
    auto optimizer_extension = make_uniq<OptimizerExtension>();
    optimizer_extension->optimize_function = [](OptimizerExtensionInput &input, 
                                            unique_ptr<LogicalOperator> &plan) {
        if (LineageState::capture == false || plan->type == LogicalOperatorType::LOGICAL_PRAGMA
            || plan->type == LogicalOperatorType::LOGICAL_SET) return;

        if (LineageState::debug) {
          std::cout << "Plan prior to modifications: \n" << plan->ToString() << std::endl;
        }
        plan = AddLineage(input, plan);
        if (LineageState::debug) {
          std::cout << "Plan after to modifications: \n" << plan->ToString() << std::endl;
        } 
    };

    auto &db_instance = *db.instance;
    db_instance.config.optimizer_extensions.emplace_back(*optimizer_extension);
    std::cout << "Fade extension loaded successfully.\n";
    
  	ExtensionUtil::RegisterFunction(db_instance, LineageScanFunction::GetFunctionSet());
  	ExtensionUtil::RegisterFunction(db_instance, LineageMetaFunction::GetFunctionSet());

    auto clear_lineage_fun = PragmaFunction::PragmaStatement("clear_lineage", PragmaClearLineage);
    ExtensionUtil::RegisterFunction(db_instance, clear_lineage_fun);
    
    auto enable_persist_fun = PragmaFunction::PragmaStatement("enable_persist_lineage", PragmaEnablePersistLineage);
    ExtensionUtil::RegisterFunction(db_instance, enable_persist_fun);
    
    auto disable_persist_fun = PragmaFunction::PragmaStatement("disable_persist_lineage", PragmaDisablePersistLineage);
    ExtensionUtil::RegisterFunction(db_instance, disable_persist_fun);
    
    auto enable_lineage_fun = PragmaFunction::PragmaStatement("enable_lineage", PragmaEnableLineage);
    ExtensionUtil::RegisterFunction(db_instance, enable_lineage_fun);
    
    auto disable_lineage_fun = PragmaFunction::PragmaStatement("disable_lineage", PragmaDisableLineage);
    ExtensionUtil::RegisterFunction(db_instance, disable_lineage_fun);
    
    auto prepare_lineage_fun = PragmaFunction::PragmaCall("prepare_lineage", PragmaPrepareLineage, {LogicalType::INTEGER});
    ExtensionUtil::RegisterFunction(db_instance, prepare_lineage_fun);
    
    auto whatif_fun = PragmaFunction::PragmaCall("whatif", PragmaFade, {LogicalType::INTEGER,
        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::INTEGER)});
    ExtensionUtil::RegisterFunction(db_instance, whatif_fun);
  	ExtensionUtil::RegisterFunction(db_instance, FadeReaderFunction::GetFunctionSet());
    
    // JSON replacement scan
    auto &config = DBConfig::GetConfig(*db.instance);
    config.replacement_scans.emplace_back(LineageScanFunction::ReadLineageReplacement);
}

extern "C" {
DUCKDB_EXTENSION_API void fade_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::FadeExtension>();
}

DUCKDB_EXTENSION_API const char *fade_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

} // namespace duckdb
