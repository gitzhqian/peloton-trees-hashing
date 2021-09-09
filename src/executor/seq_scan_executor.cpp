//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.cpp
//
// Identification: src/executor/seq_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/seq_scan_executor.h"

#include <include/storage/tuple_iterator.h>


#include "common/container_tuple.h"
#include "common/internal_types.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/create_plan.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "storage/tile.h"
#include "type/value_factory.h"
#include "expression/parameter_value_expression.h"


namespace peloton {
namespace executor {
std::vector<std::vector<bool>> SeqScanExecutor::tile_tuple_visible;
/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();

  node.GetAttributes(ais);


  target_table_ = node.GetTable();

  current_tile_group_offset_ = START_OID;

  old_predicate_ = predicate_;

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }
  table_id = target_table_->GetOid();
  table_name = target_table_->GetName();
  database_id = target_table_->GetDatabaseOid();
  tile_tuple_visible ={};
  seq_scan_type = GetSeqScanType();
  current_tile_group_offset = 0;
  total_tuple = target_table_->GetTupleCount();
//  LOG_DEBUG("seq_scan_type, %u",seq_scan_type) ;
//  if(seq_scan_type == SeqScanType::HEAPLISTSCANINDEX ){
//    tile_groups_ = target_table_->GetTileGroupList(table_tile_group_count_);
//    current_tile_group_offset_ = 0;
//  }
//  if(seq_scan_type == SeqScanType::HEAPTREESCANINDEX){
//    tile_groups_ = target_table_->GetTileGroupsBTree(table_id, table_tile_group_count_);
//    current_tile_group_offset_ = 0;
    //partition id compute, partition key{cl0,cl2,,,,,,}
    //we now assume tuple_key
//    bool isPoint = false;
//    low_ = type::ValueFactory::GetIntegerValue(0);
//    high_ = type::ValueFactory::GetIntegerValue(table_tile_group_count_);
//    if (predicate_ != nullptr) {
//      size_t pred_info_num = predicate_infos.size();
//      for (size_t cl = 0; cl < pred_info_num; cl++) {
//        Predicate_Inf preinfo = predicate_infos[cl];
//        if(preinfo.comparison_operator == (int)ExpressionType::COMPARE_EQUAL){
//          low_ = preinfo.predicate_value;
////          isPoint = true;
//          break;
//        }
//        if(preinfo.comparison_operator == (int)ExpressionType::COMPARE_GREATERTHAN){
//          low_ = preinfo.predicate_value;
//        }
//        if(preinfo.comparison_operator == (int)ExpressionType::COMPARE_LESSTHAN){
//          high_ =preinfo.predicate_value;
//        }
//      }
//    }
//    std::vector<ItemPointer *> tile_group_tree_vls =
//        target_table_->GetTileGroupBwTree(table_id,low_,high_,isPoint);
//    size_t tg_count = tile_group_tree_vls.size();
//    for(size_t i=0; i<tg_count; i++){
//      ItemPointer item = *tile_group_tree_vls[i];
//      storage::TileGroup *tg = item.tile_group_location_;
//      tile_groups_.push_back(tg);
////      LOG_DEBUG("tableid, %u, tile group id, %u",table_id_item,(tg->GetTileGroupId())) ;
//    }
//  }
//  if(seq_scan_type == SeqScanType::HEAPTREESCANFUL){
//    tile_groups_ = target_table_->GetTileGroupBwTreeFull();
//  }

  return true;
}

/**
 * @brief Creates logical tile from tile group and applies scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {

  // Scanning over a logical tile.
  if (children_.size() == 1 &&
      // There will be a child node on the create index scenario,
      // but we don't want to use this execution flow
      !(GetRawNode()->GetChildren().size() > 0 &&
        GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
            PlanNodeType::CREATE &&
        ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                ->GetCreateType() == CreateType::INDEX)) {
    // FIXME Check all requirements for children_.size() == 0 case.
    LOG_TRACE("Seq Scan executor :: 1 child ");

    PELOTON_ASSERT(target_table_ == nullptr);
    PELOTON_ASSERT(column_ids_.size() == 0);

    while (children_[0]->Execute()) {
      std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

      if (predicate_ != nullptr) {
        // Invalidate tuples that don't satisfy the predicate.
        for (oid_t tuple_id : *tile) {
          ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
          if (eval.IsFalse()) {
            // if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
            //        .IsFalse()) {
            tile->RemoveVisibility(tuple_id);
          }
        }
      }

      if (0 == tile->GetTupleCount()) {  // Avoid returning empty tiles
        continue;
      }

      /* Hopefully we needn't do projections here */
      SetOutput(tile.release());
      return true;
    }
    return false;
  }
  // Scanning a table
  else if (children_.size() == 0 ||
           // If we are creating an index, there will be a child
           (children_.size() == 1 &&
            // This check is only needed to pass seq_scan_test
            // unless it is possible to add a executor child
            // without a corresponding plan.
            GetRawNode()->GetChildren().size() > 0 &&
            // Check if the plan is what we actually expect.
            GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
                PlanNodeType::CREATE &&
            // If it is, confirm it is for indexes
            ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                    ->GetCreateType() == CreateType::INDEX)) {
    LOG_TRACE("Seq Scan executor :: 0 child ");

    PELOTON_ASSERT(target_table_ != nullptr);
    PELOTON_ASSERT(column_ids_.size() > 0);
    if (children_.size() > 0 && !index_done_) {
      children_[0]->Execute();
      // This stops continuous executions due to
      // a parent and avoids multiple creations
      // of the same index.
      index_done_ = true;
    }

//    concurrency::TransactionManager &transaction_manager =
//        concurrency::TransactionManagerFactory::GetInstance();

//    bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();
//    auto current_txn = executor_context_->GetTransaction();


    if(seq_scan_type == SeqScanType::HEAPARRAYSCAN){
      // Retrieve next tile group.
      while (current_tile_group_offset_ < table_tile_group_count_) {
        auto tile_group =
            target_table_->GetTileGroup(current_tile_group_offset_++);
//        auto tile_group_header = tile_group->GetHeader();

        oid_t active_tuple_count = tile_group->GetNextTupleSlot();

        // Construct position list by looping through tile group
        // and applying the predicate.
        std::vector<oid_t> position_list;
        for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
//          ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

//          auto visibility = transaction_manager.IsVisible(
//              current_txn, tile_group_header, tuple_id);

          // check transaction visibility
//          if (visibility == VisibilityType::OK) {
            // if the tuple is visible, then perform predicate evaluation.
            if (predicate_ == nullptr) {
              position_list.push_back(tuple_id);
//              auto res = transaction_manager.PerformRead(
//                  current_txn, location, tile_group_header, acquire_owner);
//              if (!res) {
//                transaction_manager.SetTransactionResult(current_txn,
//                                                         ResultType::FAILURE);
//                return res;
//              }
            } else {
              ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                       tuple_id);
              LOG_TRACE("Evaluate predicate for a tuple");
              auto eval =
                  predicate_->Evaluate(&tuple, nullptr, executor_context_);
              LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
              if (eval.IsTrue()) {
                position_list.push_back(tuple_id);
//                auto res = transaction_manager.PerformRead(
//                    current_txn, location, tile_group_header, acquire_owner);
//                if (!res) {
//                  transaction_manager.SetTransactionResult(current_txn,
//                                                           ResultType::FAILURE);
//                  return res;
//                } else {
//                  LOG_TRACE("Sequential Scan Predicate Satisfied");
//                }
              }
            }
//          }
        }

        // Don't return empty tiles
        if (position_list.size() == 0) {
          continue;
        }

        // Construct logical tile.
        std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
        logical_tile->AddColumns(tile_group, column_ids_);
        logical_tile->AddPositionList(std::move(position_list));

        LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
        SetOutput(logical_tile.release());
        return true;
      }
    }else if(seq_scan_type == SeqScanType::HEAPTREESCAN){
      while (current_tile_group_offset_ < table_tile_group_count_) {
        auto tile_group = target_table_->GetTileGroupBTree(table_id,current_tile_group_offset_++);;
//        current_tile_group_offset_++;

        oid_t active_tuple_count = tile_group->GetNextTupleSlot();

        // Construct position list by looping through tile group
        // and applying the predicate.
        std::vector<oid_t> position_list;
        for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
//          ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

//          auto visibility = transaction_manager.IsVisible(
//              current_txn, tile_group_header, tuple_id);

          // check transaction visibility
//          if (visibility == VisibilityType::OK) {
          // if the tuple is visible, then perform predicate evaluation.
          if (predicate_ == nullptr) {
            position_list.push_back(tuple_id);
//              auto res = transaction_manager.PerformRead(
//                  current_txn, location, tile_group_header, acquire_owner);
//              if (!res) {
//                transaction_manager.SetTransactionResult(current_txn,
//                                                         ResultType::FAILURE);
//                return res;
//              }
          } else {
            ContainerTuple<storage::TileGroup> tuple(tile_group, tuple_id);
            LOG_TRACE("Evaluate predicate for a tuple");
            auto eval =
                predicate_->Evaluate(&tuple, nullptr, executor_context_);
            LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
            if (eval.IsTrue()) {
              position_list.push_back(tuple_id);
//                auto res = transaction_manager.PerformRead(
//                    current_txn, location, tile_group_header, acquire_owner);
//                if (!res) {
//                  transaction_manager.SetTransactionResult(current_txn,
//                                                           ResultType::FAILURE);
//                  return res;
//                } else {
//                  LOG_TRACE("Sequential Scan Predicate Satisfied");
//                }
            }
          }
//          }
        }

        // Don't return empty tiles
        if (position_list.size() == 0) {
          continue;
        }

        // Construct logical tile.
        std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
        logical_tile->AddColumns(tile_group, column_ids_);
        logical_tile->AddPositionList(std::move(position_list));

        LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
        SetOutput(logical_tile.release());
        return true;
      }
    }
    else if(seq_scan_type == SeqScanType::HEAPTREESCANINDEX){
      // added by zhangqian on 2021-05
      // above scan table by block hash table, array
      // 01.scan table by block list
      // low_ = type::ValueFactory::GetIntegerValue(0);
      GetPredicateInfo(predicate_infos, predicate_);
      size_t pred_info_num = predicate_infos.size();
      type::Value min;
      type::Value max;
      bool _point =false;
      CuckooMap<oid_t, std::pair<type::Value,type::Value>> &index_= target_table_->GetSparseIndex();;
      if(predicate_ != nullptr){
        if(pred_info_num == 1){
          min = predicate_infos[0].predicate_value ;
          _point = true;
        }else{
          min = predicate_infos[0].predicate_value;
          max = predicate_infos[1].predicate_value ;
          _point = false;
        }
      }

      while (current_tile_group_offset_ < table_tile_group_count_) {
//        auto tile_group = tile_groups_[current_tile_group_offset_];
//        current_tile_group_offset_++;
        auto tile_group = target_table_->GetTileGroupBTree(table_id,current_tile_group_offset_++);;

        oid_t active_tuple_count = tile_group->GetNextTupleSlot();
        // Construct position list by looping through tile group
        // and applying the predicate.
        std::vector<oid_t> position_list;

        if(predicate_ != nullptr){
          std::pair<type::Value,type::Value> min_max;
          index_.Find(current_tile_group_offset_,min_max) ;
          if((_point == true) && ((min.CompareLessThan(min_max.first)==CmpBool::CmpTrue) || (min.CompareGreaterThan(min_max.second)==CmpBool::CmpTrue))){
            continue;
          }
          if((_point == false) && ((min.CompareGreaterThan(min_max.second)==CmpBool::CmpTrue) || (max.CompareLessThan(min_max.first) == CmpBool::CmpTrue))){
            continue;
          }
          for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
            ContainerTuple<storage::TileGroup> tuple(tile_group, tuple_id);
            LOG_TRACE("Evaluate predicate for a tuple");
            auto eval =
                predicate_->Evaluate(&tuple, nullptr, executor_context_);
            LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
            if (eval.IsTrue()) {
              position_list.push_back(tuple_id);
            }
          }
        }else{
          for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
              position_list.push_back(tuple_id);
          }
        }

        // Don't return empty tiles
        if (position_list.size() == 0) {
          continue;
        }

        // Construct logical tile.
        std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
        logical_tile->AddColumns(tile_group, column_ids_);
        logical_tile->AddPositionList(std::move(position_list));

        LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
        SetOutput(logical_tile.release());
        return true;
      }
    }else if(seq_scan_type == SeqScanType::GOOGLEBTREESCAN || seq_scan_type == SeqScanType::MASSBTREESCAN
               || seq_scan_type == SeqScanType::HOPSCOTCHMAPSCAN || seq_scan_type == SeqScanType::CUCKOOMAPSCAN){
      // 10. scan table by google b-tree
      // 10-00. data ara organized in DSM, one tile group contains the column(i)
      // 10-01. fetch all data of the column predicate
      // 10-02. check the transaction visible, check the predicate evaluate
      //build a tuple visible bit-map of the tile group
      //check the transaction visibility
      //check the predicate evaluate
      //expression: conjunction, compare
      oid_t tile_group_st = 0;
      oid_t tile_group_ed = table_tile_group_count_;
      GetPredicateInfo(predicate_infos, predicate_);
      std::vector<std::pair<oid_t ,oid_t>> tile_map_ = {};
      if (predicate_ != nullptr) {
        size_t pred_info_num = predicate_infos.size();
        if(pred_info_num == 1){
          Predicate_Inf preinfo = predicate_infos[0];
          low_ = preinfo.predicate_value;
          char *val_binary = new char[sizeof(uint32_t)];
          low_.SerializeTo(val_binary, true, nullptr);
          uint32_t k_low_ = *reinterpret_cast<const int32_t *>(val_binary);
          tile_group_l_ = k_low_/TEST_TUPLES_PER_TILEGROUP;

          is_point = true;
          tile_group_st = tile_group_l_;
          total_tuple = 1;
          tile_map_.push_back(std::make_pair((k_low_%TEST_TUPLES_PER_TILEGROUP),tile_group_st));
        }else{
          for (size_t cl = 0; cl < pred_info_num; cl++) {
            Predicate_Inf preinfo = predicate_infos[cl];
            if(preinfo.comparison_operator == (int)ExpressionType::COMPARE_GREATERTHAN){
              low_ = preinfo.predicate_value;
            }
            if(preinfo.comparison_operator == (int)ExpressionType::COMPARE_LESSTHAN){
              high_ = preinfo.predicate_value;
            }
          }
          char *val_binary = new char[sizeof(uint32_t)];
          low_.SerializeTo(val_binary, true, nullptr);
          uint32_t k_low_ = *reinterpret_cast<const int32_t *>(val_binary);
          tile_group_l_ = k_low_/TEST_TUPLES_PER_TILEGROUP;

          char *val_binary_h = new char[sizeof(uint32_t)];
          high_.SerializeTo(val_binary_h, true, nullptr);
          uint32_t k_high_ = *reinterpret_cast<const int32_t *>(val_binary_h);
          tile_group_h_ = k_high_/TEST_TUPLES_PER_TILEGROUP;

          tile_group_st = tile_group_l_;
          tile_group_ed = tile_group_h_;
          k_high_ - k_low_==0 ? total_tuple=0: total_tuple=(k_high_ - k_low_ - 1);
          is_point=false;

          tile_map_.push_back(std::make_pair((k_low_%TEST_TUPLES_PER_TILEGROUP),tile_group_st));
          tile_map_.push_back(std::make_pair((k_high_%TEST_TUPLES_PER_TILEGROUP),tile_group_ed));
        }
      }
      //2. if need transaction visiblity, we now assume all is visibility
      //   we consider to check which partition is dirty
      //3. if is in the column_ids_ out
      //4. else is the predicate
      // Construct logical tile.
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
      logical_tile->AddTableColumns(table_id, column_ids_,database_id);
      logical_tile->AddTileTupleVisible(tile_map_,total_tuple,tile_group_st,tile_group_ed,0,is_point);
      SetOutput(logical_tile.release());

      return false;
    }

  }
  return false;
}

// Update Predicate expression
// this is used in the NLJoin executor
void SeqScanExecutor::UpdatePredicate(const std::vector<oid_t> &column_ids,
                                      const std::vector<type::Value> &values) {
  std::vector<oid_t> predicate_column_ids;

  PELOTON_ASSERT(column_ids.size() <= column_ids_.size());

  // columns_ids is the column id
  // in the join executor, should
  // convert to the column id in the
  // seq scan executor
  for (auto column_id : column_ids) {
    predicate_column_ids.push_back(column_ids_[column_id]);
  }

  expression::AbstractExpression *new_predicate =
      values.size() != 0 ? ColumnsValuesToExpr(predicate_column_ids, values, 0)
                         : nullptr;

  // combine with original predicate
  if (old_predicate_ != nullptr) {
    expression::AbstractExpression *lexpr = new_predicate,
                                   *rexpr = old_predicate_->Copy();

    new_predicate = new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, lexpr, rexpr);
  }

  // Currently a hack that prevent memory leak
  // we should eventually make prediate_ a unique_ptr
  new_predicate_.reset(new_predicate);
  predicate_ = new_predicate;
}

// Transfer a list of equality predicate
// to a expression tree
expression::AbstractExpression *SeqScanExecutor::ColumnsValuesToExpr(
    const std::vector<oid_t> &predicate_column_ids,
    const std::vector<type::Value> &values, size_t idx) {
  if (idx + 1 == predicate_column_ids.size())
    return ColumnValueToCmpExpr(predicate_column_ids[idx], values[idx]);

  // recursively build the expression tree
  expression::AbstractExpression *lexpr = ColumnValueToCmpExpr(
                                     predicate_column_ids[idx], values[idx]),
                                 *rexpr = ColumnsValuesToExpr(
                                     predicate_column_ids, values, idx + 1);

  expression::AbstractExpression *root_expr =
      new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
                                            lexpr, rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}

expression::AbstractExpression *SeqScanExecutor::ColumnValueToCmpExpr(
    const oid_t column_id, const type::Value &value) {
  expression::AbstractExpression *lexpr =
      new expression::TupleValueExpression("");
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetValueType(
      target_table_->GetSchema()->GetColumn(column_id).GetType());
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)
      ->SetValueIdx(column_id);

  expression::AbstractExpression *rexpr =
      new expression::ConstantValueExpression(value);

  expression::AbstractExpression *root_expr =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, lexpr,
                                           rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}

type::Value SeqScanExecutor::ComparisonFilter(
    const type::Value &value_, Predicate_Inf &predicate_) {
  PELOTON_ASSERT(children_.size() == 2);
  auto vl = value_;
  type::Value vr = predicate_.predicate_value;
  int exp_type_ = predicate_.comparison_operator;
  switch (exp_type_) {
    case (int)(ExpressionType::COMPARE_EQUAL):
      return type::ValueFactory::GetBooleanValue(vl.CompareEquals(vr));
    case (int)(ExpressionType::COMPARE_NOTEQUAL):
      return type::ValueFactory::GetBooleanValue(vl.CompareNotEquals(vr));
    case (int)(ExpressionType::COMPARE_LESSTHAN):
      return type::ValueFactory::GetBooleanValue(vl.CompareLessThan(vr));
    case (int)(ExpressionType::COMPARE_GREATERTHAN):
      return type::ValueFactory::GetBooleanValue(vl.CompareGreaterThan(vr));
    case (int)(ExpressionType::COMPARE_LESSTHANOREQUALTO):
      return type::ValueFactory::GetBooleanValue(vl.CompareLessThanEquals(vr));
    case (int)(ExpressionType::COMPARE_GREATERTHANOREQUALTO):
      return type::ValueFactory::GetBooleanValue(
          vl.CompareGreaterThanEquals(vr));
    case (int)(ExpressionType::COMPARE_DISTINCT_FROM): {
      if (vl.IsNull() && vr.IsNull()) {
        return type::ValueFactory::GetBooleanValue(false);
      } else if (!vl.IsNull() && !vr.IsNull()) {
        return type::ValueFactory::GetBooleanValue(vl.CompareNotEquals(vr));
      }
      return type::ValueFactory::GetBooleanValue(true);
    }
    default:
      throw Exception("Invalid comparison expression type.");
  }
}

void SeqScanExecutor::GetPredicateInfo(
    std::vector<Predicate_Inf> &infos,
    const expression::AbstractExpression *expr) {
  if (expr != nullptr) {
//    size_t child_num = expr->GetChildrenSize();
    // If its and, split children and parse again
//    for (size_t child = 0; child < child_num; child++) {
      auto expr_type = expr->GetExpressionType();
      if (expr_type == ExpressionType::CONJUNCTION_AND) {
         const expression::AbstractExpression *expr_l = expr->GetChild(0);
         GetPredicateInfo(infos, expr_l);
         const expression::AbstractExpression *expr_r = expr->GetChild(1);
         GetPredicateInfo(infos, expr_r);
      } else if (expr_type == ExpressionType::COMPARE_EQUAL ||
                 expr_type == ExpressionType::COMPARE_LESSTHAN ||
                 expr_type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
                 expr_type == ExpressionType::COMPARE_GREATERTHAN ||
                 expr_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
        // The right child should be a constant.
        auto right_child = expr->GetChild(1);

        if (right_child->GetExpressionType() == ExpressionType::VALUE_CONSTANT ||
            right_child->GetExpressionType() == ExpressionType::VALUE_PARAMETER) {
          type::Value predicate_val;
          if(right_child->GetExpressionType() == ExpressionType::VALUE_CONSTANT){
            auto right_exp = (const expression::ConstantValueExpression *)(expr->GetChild(1));
            predicate_val = right_exp->GetValue();
          }else{
            auto right_exp = (const expression::ParameterValueExpression *)(expr->GetChild(1));
            predicate_val = this->executor_context_->GetParamValues().at(right_exp->GetValueIdx());
          }

          // Get the column id for this predicate
          auto left_exp = (const expression::TupleValueExpression *)(expr->GetChild(0));
          int col_id = left_exp->GetColumnId();

          auto comparison_operator = (int)expr->GetExpressionType();

          Predicate_Inf pre_info{col_id,comparison_operator,predicate_val};

          infos.push_back(pre_info);
        }
      }
    }
//  }

}

}  // namespace executor
}  // namespace peloton
