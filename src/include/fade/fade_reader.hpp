#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

struct FadeReaderBindData : public TableFunctionData {
  idx_t cardinality;
  idx_t chunk_count;

  void Initialize() {
    cardinality = 0;
    chunk_count = 0;
  }
};

struct FadeReaderLocalState : public LocalTableFunctionState {
};

struct FadeReaderGlobalState : public GlobalTableFunctionState {
};

class FadeReaderFunction {
  public:
    static void FadeReaderImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> FadeReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> FadeReaderInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState>
    FadeReaderInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
    static unique_ptr<TableRef> FadeReaderReplacement(ClientContext &context, ReplacementScanInput &input,
        optional_ptr<ReplacementScanData> data);
    static TableFunctionSet GetFunctionSet();
    static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data);
    static unique_ptr<BaseStatistics> ScanStats(ClientContext &context, const FunctionData *bind_data_p,
        column_t column_index);
};

} // namespace duckdb
