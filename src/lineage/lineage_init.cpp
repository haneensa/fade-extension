#include "lineage/lineage_init.hpp"

#include <iostream>

#include "fade_extension.hpp"
#include "lineage/logical_lineage_operator.hpp"

#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

idx_t LineageState::query_id = 0;
idx_t LineageState::global_id = 0;
bool LineageState::capture = false;
bool LineageState::debug = false;
bool LineageState::persist = true;
idx_t LineageState::table_idx = 0;
std::unordered_map<string, vector<std::pair<Vector, int>>> LineageState::lineage_store;
std::unordered_map<string, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> LineageState::qid_plans;
std::unordered_map<idx_t, idx_t> LineageState::qid_plans_roots;


AggregateFunction GetListFunction(ClientContext &context) {
  auto &catalog = Catalog::GetSystemCatalog(context);
  auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
      context, DEFAULT_SCHEMA, "list"
  );
  return entry.functions.GetFunctionByArguments(context, {LogicalType::ROW_TYPE});
}

idx_t ProcessJoin(unique_ptr<LogicalOperator> &op, vector<idx_t>& rowids, idx_t query_id) {
  auto &join = op->Cast<LogicalComparisonJoin>();
  idx_t left_col_id = 0;
  idx_t right_col_id = 0;
  if (LineageState::debug)
  std::cout << "Process join: " << EnumUtil::ToChars<JoinType>(join.join_type) << std::endl;
  //std::cout << "LEFT " << join.children[0]->ToString() << std::endl;
  //std::cout << "RIGHT " << join.children[1]->ToString() << std::endl;
  if (join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
    if (LineageState::debug)
    std::cout << "inject right semi join: " << rowids[1] << " " << join.right_projection_map.size() << std::endl;
    if (!join.right_projection_map.empty()) {
      right_col_id = join.right_projection_map.size();
      join.right_projection_map.push_back(rowids[1]);
    } else {
      right_col_id = rowids[1];
    }

    if (LineageState::debug)
    std::cout << "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id << " " << rowids[0] << " " << rowids[1] << " "
      << join.left_projection_map.size() << " " << join.right_projection_map.size() << std::endl;
    int source_count = 1;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++,
        query_id, op->type, source_count,  0, right_col_id);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return right_col_id;
  }

  if (!join.left_projection_map.empty()) {
    left_col_id = join.left_projection_map.size();
    join.left_projection_map.push_back(rowids[0]);
  } else {
    left_col_id = rowids[0];
  }

  if (join.join_type == JoinType::MARK) {
      if (LineageState::debug)
    std::cout << "inject mark join: " << left_col_id << " " << join.left_projection_map.size() << std::endl;
    int source_count = 1;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id,
        op->type, source_count, left_col_id, 0);
    lop->AddChild(std::move(op));
    lop->mark_join = true;
    op = std::move(lop);
    // add projection?
    return left_col_id+1 /* bool col */;
  } else if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {
      if (LineageState::debug)
    std::cout << "inject semi join: " << left_col_id << " " << join.left_projection_map.size() << std::endl;
    int source_count = 1;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id,
        op->type, source_count, left_col_id, 0);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return left_col_id;
  }

  if (!join.right_projection_map.empty()) {
    right_col_id = join.right_projection_map.size();
    join.right_projection_map.push_back(rowids[1]);
  } else {
    right_col_id = rowids[1];
  }

  if (LineageState::debug)
  std::cout << "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id << " " << rowids[0] << " " << rowids[1] << " " << join.left_projection_map.size() << " " << join.right_projection_map.size() << std::endl;
  int source_count = 2;
  auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id,
      op->type, source_count, left_col_id, right_col_id);
  lop->AddChild(std::move(op));
  op = std::move(lop);
  return left_col_id + right_col_id;
}

idx_t InjectLineageOperator(unique_ptr<LogicalOperator> &op,ClientContext &context, idx_t query_id) {
  if (!op) return 0;
  vector<idx_t> rowids = {};

  // maybe init this in LogicalLineageOperator ? this way we only keep track of importan operators and exclude projections
  // or traverse query plan again, and construct the query plan pointers?

  for (auto &child : op->children) {
    rowids.push_back( InjectLineageOperator(child, context,  query_id) );
  }

  if (LineageState::debug) {
    std::cout << "Inject: " << op->GetName() << " " << LineageState::global_id;
    for (int i = 0; i < rowids.size(); ++i)  std::cout << " -> " << rowids[i];
    std::cout << std::endl;
  }
  if (op->type == LogicalOperatorType::LOGICAL_GET) {
    // leaf node. add rowid attribute to propagate.
    auto &get = op->Cast<LogicalGet>();
    get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
    LineageState::table_idx = get.table_index;
    idx_t col_id  =  get.GetColumnIds().size() - 1;
    // projection_ids index into column_ids. if any exist then reference new column
    if (!get.projection_ids.empty()) {
      get.projection_ids.push_back(col_id);
      col_id = get.projection_ids.size() - 1;
    }
    if (LineageState::debug)
      std::cout << "LogicalGet: " << col_id << " " << op->expressions.size() << " " << get.names.size() << " " << get.projection_ids.size() <<
        " " << get.returned_types.size() <<  " " << get.types.size() << " " << get.GetColumnIds().size() << "\n";
    return col_id;
  }  else if (op->type == LogicalOperatorType::LOGICAL_CHUNK_GET) { // CTE_SCAN too
    // add lineage op to generate ids
    auto& col = op->Cast<LogicalColumnDataGet>();
    idx_t col_id = col.chunk_types.size();
    if (LineageState::debug) std::cout << "chunk get " << col_id << std::endl;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id,
        op->type, 1, col_id, 0);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return col_id;
  }  else if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) { // CTE_SCAN too
      if (LineageState::debug)  std::cout << " cte ? " << rowids[0] << " " << rowids[1] << std::endl;
      return rowids[1];
  }  else if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) { // CTE_SCAN too
    // add lineage op to generate ids
    auto& col = op->Cast<LogicalCTERef>();
    idx_t col_id = col.chunk_types.size();
    if (LineageState::debug) std::cout << "cte ref " << col_id << " " << col.bound_columns[0] << " " << std::endl;
    col.chunk_types.push_back(LogicalType::ROW_TYPE);
    col.bound_columns.push_back("rowid");
    //auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, col_id, 0);
    //lop->AddChild(std::move(op));
    //op = std::move(lop);
    return col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
    auto &filter = op->Cast<LogicalFilter>();
    int col_id = rowids[0];
    int new_col_id = col_id;

    if (op->children[0]->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
      // check if the child is mark join
      if (op->children[0]->Cast<LogicalLineageOperator>().mark_join) {
          // pull up lineage op
          auto lop = std::move(op->children[0]);
          if (LineageState::debug)
            std::cout << "pull up lineage op " << rowids[0] << " " 
          << filter.expressions.size() << " " << filter.projection_map.size() << " " << 
          lop->Cast<LogicalLineageOperator>().left_rid << std::endl;
          lop->Cast<LogicalLineageOperator>().dependent_type = op->type;
    
          idx_t child_left_rid = lop->Cast<LogicalLineageOperator>().left_rid;
          if (!filter.projection_map.empty()) {
            // annotations, but projection_map refer to the extra bool column that we need to adjust
            // the last column is the boolean
            filter.projection_map.push_back(child_left_rid+1);
            lop->Cast<LogicalLineageOperator>().left_rid = filter.projection_map.size()-2; 
            new_col_id = filter.projection_map.size()-1; 
          }
          op->children[0] = std::move(lop->children[0]);
          lop->children[0] = std::move(op);
          op = std::move(lop);
          return new_col_id;
      }
    }
    if (!filter.projection_map.empty()) {
        filter.projection_map.push_back(col_id); 
        new_col_id = filter.projection_map.size()-1; 
    }
    if (LineageState::debug)
    std::cout << "Filter " << filter.expressions.size() << " " << filter.projection_map.size() << " " << col_id << " " << new_col_id << std::endl;
    return new_col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
    // it passes through child types. except if projections is not empty, then we need to add it
    auto &order = op->Cast<LogicalOrder>();
    if (LineageState::debug)
    std::cout << "Order by " << order.projection_map.size() << " " << rowids[0] << std::endl;
    if (!order.projection_map.empty()) {
     // order.projections.push_back(); the rowid of child
    }
    return rowids[0];
  } else if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
    // passes through child types
    return rowids[0];
  } else if (op->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
    if (rowids.size() > 0)  return rowids[0];
    else return 0;
  } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
    // projection, just make sure we propagate any annotation columns
    int col_id = rowids[0];
    op->expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));
    int new_col_id = op->expressions.size()-1;
    if (LineageState::debug)
      std::cout << "[DEBUG] Projection types before modification: " << col_id << " " << new_col_id  << "\n";
    return new_col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
    // duplicate eliminated scan (output of distinct)
    auto &get = op->Cast<LogicalDelimGet>();
    if (LineageState::debug)
      std::cout << "LogicalDelimGet types after injection: " << get.table_index << " " << get.chunk_types.size() << std::endl;
    int col_id = get.chunk_types.size();
    get.chunk_types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id,
        op->type, 1, col_id, 0);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return col_id; // TODO: adjust once I adjust distinct types
  } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    // the JOIN right child, becomes right_delim_join child that is used as input to
    // JOIN and DISTINCT
    // 1) access to distinct to add LIST(rowid) expression
    // 2) JOIN to add annotations from both sides
    // the fist n childrens are n delim scans
    return ProcessJoin(op, rowids, query_id);
  } else if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
  } else if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
  } else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
    // check op.join_type = {SEMI, ANTI, RIGHT_ANTI, RIGHT_SEMI, MARK}
    // Propagate annotations from the left and right sides.
    // Add PhysicaLineage to extraxt the last two columns
    // and replace it with a single annotations column
    return ProcessJoin(op, rowids, query_id);
  } else if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
      auto &aggr = op->Cast<LogicalAggregate>();
     // if (!aggr.groups.empty()) {
          if (LineageState::debug) std::cout << "[DEBUG] Modifying Aggregate operator\n";
          auto list_function = GetListFunction(context);
          auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::ROW_TYPE, rowids[0]);
          vector<unique_ptr<Expression>> children;
          children.push_back(std::move(rowid_colref));
          unique_ptr<FunctionData> bind_info = list_function.bind(context, list_function, children);
          auto list_aggregate = make_uniq<BoundAggregateExpression>(
              list_function, std::move(children), nullptr, std::move(bind_info),
              AggregateType::NON_DISTINCT
          );

          aggr.expressions.push_back(std::move(list_aggregate));
          idx_t new_col_id = aggr.groups.size() + aggr.expressions.size() + aggr.grouping_functions.size() - 1;
          
          auto dummy = make_uniq<LogicalLineageOperator>(aggr.estimated_cardinality, LineageState::global_id++, query_id,
              op->type, 1, new_col_id, 0);
          dummy->AddChild(std::move(op));

          op = std::move(dummy);
          
          return new_col_id;
    //  } // if simple agg, add operator below to remove annotations, and operator above to generate annotations
  }
  return 0;
}

idx_t BuildLineageInfoTree(unordered_map<idx_t, unique_ptr<LineageInfoNode>>&lop_plan, unique_ptr<LogicalOperator>&plan) {
  if (plan->children.size() == 0) {
    idx_t opid = LineageState::global_id++;
    lop_plan[opid] =  make_uniq<LineageInfoNode>(opid, plan->type);
    return opid;
  }
  
  // if this is an extension for lineage then get left and right childrens
  idx_t child = BuildLineageInfoTree(lop_plan, plan->children[0]);

  if (plan->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
    idx_t opid = plan->Cast<LogicalLineageOperator>().operator_id;
    auto lop = make_uniq<LineageInfoNode>(opid,
        plan->Cast<LogicalLineageOperator>().dependent_type);
    lop->children.push_back(child);
    if (plan->children[0]->children.size() > 1) {
      idx_t rhs_child = BuildLineageInfoTree(lop_plan, plan->children[0]->children[1]);
      lop->children.push_back(rhs_child);
    }
    
    if (lop->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    }
    lop_plan[opid] = std::move(lop);
    return opid;
  }

  return child;
}
  
unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                    unique_ptr<LogicalOperator>& plan) {
  idx_t query_id = LineageState::query_id++; 
  LineageState::global_id = 0;
  idx_t final_rowid = InjectLineageOperator(plan, input.context, query_id);
  // inject lineage op at the root of the plan to extract any annotation columns
  // If root is create table, then add lineage operator below it
  auto root = make_uniq<LogicalLineageOperator>(plan->estimated_cardinality, LineageState::global_id++, query_id, plan->children[0]->type, final_rowid, 0, true);
  LineageState::qid_plans_roots[query_id] = root->operator_id;
  if (plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
    auto child = std::move(plan->children[0]);
    root->AddChild(std::move(child));
    plan->children[0] = std::move(root);
  } else {
    root->AddChild(std::move(plan));
    plan = std::move(root);
  }
  BuildLineageInfoTree(LineageState::qid_plans[query_id], plan);

  return std::move(plan);
}

} // namespace duckdb
