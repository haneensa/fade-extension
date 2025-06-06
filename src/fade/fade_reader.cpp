#include "fade/fade_reader.hpp"

#include "fade_extension.hpp"
#include "lineage/lineage_init.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {

void FadeReaderFunction::FadeReaderImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  if (!data_p.local_state) return;
  auto &data = data_p.local_state->Cast<FadeReaderLocalState>();
  auto &gstate = data_p.global_state->Cast<FadeReaderGlobalState>();
  auto &bind_data = data_p.bind_data->CastNoConst<FadeReaderBindData>();

  std::cout << "fade reader implementation" <<  LineageState::capture << " " << LineageState::query_id << " " << LineageState::persist << std::endl;
  if (bind_data.chunk_count > 0) return;

  output.data[0].Sequence(0, 1, LineageState::query_id % STANDARD_VECTOR_SIZE);

  output.SetCardinality(LineageState::query_id);


  bind_data.chunk_count++;
}

// table name: lineage_scan(table_name)
unique_ptr<FunctionData> FadeReaderFunction::FadeReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<FadeReaderBindData>();

  // optional: provide a query (equivalent to filter push down). if 'ALL' then return all.
  // result->table_name = query;

  names.emplace_back("query_id");
  return_types.emplace_back(LogicalType::INTEGER);
  
  //names.emplace_back("plan");
	//return_types.emplace_back(LogicalType::VARCHAR);

  return std::move(result);
}

unique_ptr<LocalTableFunctionState>
FadeReaderFunction::FadeReaderInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
    GlobalTableFunctionState *gstate_p) {
  return make_uniq<FadeReaderLocalState>();
}

unique_ptr<GlobalTableFunctionState> FadeReaderFunction::FadeReaderInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    return make_uniq<FadeReaderGlobalState>();
}

unique_ptr<TableRef> FadeReaderFunction::FadeReaderReplacement(ClientContext &context, ReplacementScanInput &input,
    optional_ptr<ReplacementScanData> data) {
  auto table_name = ReplacementScan::GetFullPath(input);

  if (!ReplacementScan::CanReplace(table_name, {"fade_reader"})) {
    return nullptr;
  }
  
  // if it has lineage as prefix
  auto table_function = make_uniq<TableFunctionRef>();
  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
  table_function->function = make_uniq<FunctionExpression>("fade_reader", std::move(children));

  if (!FileSystem::HasGlob(table_name)) {
    auto &fs = FileSystem::GetFileSystem(context);
    table_function->alias = fs.ExtractBaseName(table_name);
  }

  return std::move(table_function);
}

unique_ptr<NodeStatistics> FadeReaderFunction::Cardinality(ClientContext &context, const FunctionData *bind_data) {
  auto &data = bind_data->CastNoConst<FadeReaderBindData>();
  return make_uniq<NodeStatistics>(10);
}
unique_ptr<BaseStatistics> FadeReaderFunction::ScanStats(ClientContext &context,
    const FunctionData *bind_data_p, column_t column_index) {
  auto &bind_data = bind_data_p->CastNoConst<FadeReaderBindData>();
  auto stats = NumericStats::CreateUnknown(LogicalType::ROW_TYPE);
  NumericStats::SetMin(stats, Value::BIGINT(0));
  NumericStats::SetMax(stats, Value::BIGINT(10));
  stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES); // depends on the type of operator
  return stats.ToUnique();
}

TableFunctionSet FadeReaderFunction::GetFunctionSet() {
  // table_name/query_name: VARCHAR, lineage_ids: List(INT)
  TableFunction table_function("fade_reader", {LogicalType::INTEGER}, FadeReaderImplementation,
      FadeReaderBind, FadeReaderInitGlobal, FadeReaderInitLocal);

  table_function.statistics = ScanStats;
  table_function.cardinality = Cardinality;
  table_function.projection_pushdown = true;
  table_function.filter_pushdown = false;
  table_function.filter_prune = false;
  return MultiFileReader::CreateFunctionSet(table_function);
}
  
} // namespace duckdb
