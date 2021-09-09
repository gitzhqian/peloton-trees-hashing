//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile.cpp
//
// Identification: src/executor/logical_tile.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include "executor/logical_tile.h"

#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/data_table.h"
#include "storage/layout.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "storage/storage_manager.h"
#include <include/storage/tuple_iterator.h>

namespace peloton {
namespace executor {

#define SCHEMA_PREALLOCATION_SIZE 20

LogicalTile::LogicalTile() {
  // Preallocate schema
  schema_.reserve(SCHEMA_PREALLOCATION_SIZE);
}

/**
 * @brief Get the schema of the tile.
 * @return ColumnInfo-based schema of the tile.
 */
const std::vector<LogicalTile::ColumnInfo> &LogicalTile::GetSchema() const {
  return schema_;
}

/**
 * @brief Get the information about the column.
 * @return ColumnInfo of the column.
 */
const LogicalTile::ColumnInfo &LogicalTile::GetColumnInfo(
    const oid_t column_id) const {
  return schema_[column_id];
}

/**
 * @brief Construct the underlying physical schema of all the columns in the
 *logical tile.
 *
 * @return New schema object.
 */
catalog::Schema *LogicalTile::GetPhysicalSchema() const {
  std::vector<catalog::Column> physical_columns;

  for (ColumnInfo column : schema_) {
    auto schema = column.base_tile->GetSchema();
    auto physical_column = schema->GetColumn(column.origin_column_id);
    physical_columns.push_back(physical_column);
  }

  catalog::Schema *schema = new catalog::Schema(physical_columns);

  return schema;
}

/**
 * @brief Get the position lists of the tile.
 * @return Position lists of the tile.
 */
const LogicalTile::PositionLists &LogicalTile::GetPositionLists() const {
  return position_lists_;
}

/**
 * @brief Get the position list at given offset in the tile.
 * @return Position list associated with column.
 */
const LogicalTile::PositionList &LogicalTile::GetPositionList(
    const oid_t column_id) const {
  return position_lists_[column_id];
}

/**
 * @brief Set the position lists of the tile.
 * @param Position lists.
 */
void LogicalTile::SetPositionLists(
    LogicalTile::PositionLists &&position_lists) {
  position_lists_ = position_lists;
}

void LogicalTile::SetPositionListsAndVisibility(
    LogicalTile::PositionLists &&position_lists) {
  position_lists_ = position_lists;
  if (position_lists.size() > 0) {
    total_tuples_ = position_lists[0].size();
    visible_rows_.resize(position_lists_[0].size(), true);
    visible_tuples_ = position_lists_[0].size();
  }
}

/**
 * @brief Adds position list to logical tile.
 * @param position_list Position list to be added. Note the move semantics.
 *
 * The first position list to be added determines the number of rows in this
 * logical tile.
 *
 * @return Position list index of newly added list.
 */
int LogicalTile::AddPositionList(LogicalTile::PositionList &&position_list) {
  PELOTON_ASSERT(position_lists_.size() == 0 ||
            position_lists_[0].size() == position_list.size());

  if (position_lists_.size() == 0) {
    visible_tuples_ = position_list.size();
    visible_rows_.resize(position_list.size(), true);

    // All tuples are visible initially
    total_tuples_ = visible_tuples_;
  }

  position_lists_.push_back(std::move(position_list));
  return position_lists_.size() - 1;
}
void LogicalTile::AddTileTupleVisible(std::vector<std::vector<bool>> tile_tuple_visible) {
  PELOTON_ASSERT(tile_tuple_visible_.size() == 0);

  tile_tuples_visible_ = std::move(tile_tuple_visible);
}
void LogicalTile::AddTileTupleVisible(std::vector<std::vector<bool>> tile_tuple_visible, oid_t tuple_count) {
  PELOTON_ASSERT(tile_tuple_visible_.size() == 0);

  tile_tuples_visible_ = std::move(tile_tuple_visible);
  total_tuples_ = tuple_count;
}
void LogicalTile::AddTileTupleVisible(std::vector<std::pair<oid_t ,oid_t>> tile_tuple_visible,
                                      oid_t tuple_count,
                                      oid_t tile_group_st,
                                      oid_t tile_group_ed,
                                      oid_t column_id,
                                      bool is_point) {
  PELOTON_ASSERT(tile_tuple_visible_.size() == 0);

  visible_range_ = tile_tuple_visible;
  total_tuples_ = tuple_count;
  tile_group_st_ = tile_group_st;
  tile_group_ed_ = tile_group_ed;
  column_id_ = column_id;
  is_point_= is_point;
}
void LogicalTile::AddTileTupleVisible(std::vector<oid_t> tile_tuple_visible) {
  PELOTON_ASSERT(tile_tuple_visible_.size() == 0);

  tile_tuple_visible_ = std::move(tile_tuple_visible);
}
void LogicalTile::AddPartitionTupleVisible(std::vector<std::vector<bool>> tile_tuple_visible,
                                           oid_t partition_offset) {
  PELOTON_ASSERT(tile_tuples_visible_.size() == 0);

//  partition_tuple_visible_ = std::move(partition_tuple_visible);
  tile_tuples_visible_ = tile_tuple_visible;
  partition_offset_ = partition_offset;
}

/**
 * @brief Remove visibility the specified tuple in the logical tile.
 * @param tuple_id Id of the specified tuple.
 */
void LogicalTile::RemoveVisibility(oid_t tuple_id) {
  PELOTON_ASSERT(tuple_id < total_tuples_);
  PELOTON_ASSERT(visible_rows_[tuple_id]);

  visible_rows_[tuple_id] = false;
  visible_tuples_--;
}

/**
 * @brief Returns base tile that the specified column was from.
 * @param column_id Id of the specified column.
 *
 * @return Pointer to base tile of specified column.
 */
storage::Tile *LogicalTile::GetBaseTile(oid_t column_id) {
  return schema_[column_id].base_tile.get();
}

/**
 * @brief Get the value at the specified field.
 * @param tuple_id Tuple id of the specified field (row/position).
 * @param column_id Column id of the specified field.
 *
 * @return Value at the specified field,
 *         or VALUE_TYPE_INVALID if it doesn't exist.
 */
// TODO: Deprecated. Avoid calling this function if possible.
type::Value LogicalTile::GetValue(oid_t tuple_id, oid_t column_id) {
  PELOTON_ASSERT(column_id < schema_.size());
  PELOTON_ASSERT(tuple_id < total_tuples_);

  ColumnInfo &cp = schema_[column_id];
  oid_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_id];
  storage::Tile *base_tile = cp.base_tile.get();

  if (base_tuple_id == NULL_OID) {
    return type::ValueFactory::GetNullValueByType(
        base_tile->GetSchema()->GetType(column_id));
  } else {
    return base_tile->GetValue(base_tuple_id, cp.origin_column_id);
  }
}

// this function is designed for overriding pure virtual function.
void LogicalTile::SetValue(type::Value &value UNUSED_ATTRIBUTE,
                           oid_t tuple_id UNUSED_ATTRIBUTE,
                           oid_t column_id UNUSED_ATTRIBUTE) {
  PELOTON_ASSERT(false);
}

/**
 * @brief Returns the number of visible tuples in this logical tile.
 *
 * @return Number of tuples.
 */
size_t LogicalTile::GetTupleCount() { return visible_tuples_; }

/**
 * @brief Returns the number of columns.
 *
 * @return Number of columns.
 */
size_t LogicalTile::GetColumnCount() { return schema_.size(); }
size_t LogicalTile::GetKColumnCount() { return column_ids_.size(); }

/**
 * @brief Returns iterator pointing to first tuple.
 *
 * @return iterator pointing to first tuple.
 */
LogicalTile::iterator LogicalTile::begin() {
  bool begin = true;
  return iterator(this, begin);
}

/**
 * @brief Returns iterator indicating that we are past the last tuple.
 *
 * @return iterator indicating we're past the last tuple.
 */
LogicalTile::iterator LogicalTile::end() {
  bool begin = false;
  return iterator(this, begin);
}

/**
 * @brief Constructor for iterator.
 * @param Logical tile corresponding to this iterator.
 * @param begin Specifies whether we want the iterator initialized to point
 *              to the first tuple id, or to past-the-last tuple.
 */
LogicalTile::iterator::iterator(LogicalTile *tile, bool begin) : tile_(tile) {
  if (!begin) {
    pos_ = INVALID_OID;
    return;
  }

  auto total_tile_tuples = tile_->total_tuples_;

  // Find first visible tuple.
  pos_ = 0;
  while (pos_ < total_tile_tuples && !tile_->visible_rows_[pos_]) {
    pos_++;
  }

  // If no visible tuples...
  if (pos_ == total_tile_tuples) {
    pos_ = INVALID_OID;
  }
}

/**
 * @brief Increment operator.
 *
 * It ignores invisible tuples.
 *
 * @return iterator after the increment.
 */
LogicalTile::iterator &LogicalTile::iterator::operator++() {
  auto total_tile_tuples = tile_->total_tuples_;

  // Find next visible tuple.
  do {
    pos_++;
  } while (pos_ < total_tile_tuples && !tile_->visible_rows_[pos_]);

  if (pos_ == total_tile_tuples) {
    pos_ = INVALID_OID;
  }

  return *this;
}

/**
 * @brief Increment operator.
 *
 * It ignores invisible tuples.
 *
 * @return iterator before the increment.
 */
LogicalTile::iterator LogicalTile::iterator::operator++(int) {
  LogicalTile::iterator tmp(*this);
  operator++();
  return tmp;
}

/**
 * @brief Equality operator.
 * @param rhs The iterator to compare to.
 *
 * @return True if equal, false otherwise.
 */
bool LogicalTile::iterator::operator==(const iterator &rhs) {
  return pos_ == rhs.pos_ && tile_ == rhs.tile_;
}

/**
 * @brief Inequality operator.
 * @param rhs The iterator to compare to.
 *
 * @return False if equal, true otherwise.
 */
bool LogicalTile::iterator::operator!=(const iterator &rhs) {
  return pos_ != rhs.pos_ || tile_ != rhs.tile_;
}

/**
 * @brief Dereference operator.
 *
 * @return Id of tuple that iterator is pointing at.
 */
oid_t LogicalTile::iterator::operator*() { return pos_; }

LogicalTile::~LogicalTile() {
  // Automatically drops reference on base tiles for each column
}

LogicalTile::PositionListsBuilder::PositionListsBuilder() {
  // Nothing to do here !
}

LogicalTile::PositionListsBuilder::PositionListsBuilder(
    const LogicalTile::PositionLists *left_pos_list,
    const LogicalTile::PositionLists *right_pos_list) {
  const LogicalTile::PositionLists *non_empty_pos_list = nullptr;
  if (left_pos_list == nullptr) {
    non_empty_pos_list = right_pos_list;
    SetRightSource(right_pos_list);
  } else {
    non_empty_pos_list = left_pos_list;
    SetLeftSource(left_pos_list);
  }
  PELOTON_ASSERT(non_empty_pos_list != nullptr);
  output_lists_.push_back(std::vector<oid_t>());
  // reserve one extra pos list for the empty tile
  for (size_t column_itr = 0; column_itr < non_empty_pos_list->size() + 1;
       column_itr++) {
    output_lists_.push_back(std::vector<oid_t>());
  }
}

/**
 * Initialize the position list of result tiles based on the number of
 * columns of the left and right tiles
 */
LogicalTile::PositionListsBuilder::PositionListsBuilder(LogicalTile *left_tile,
                                                        LogicalTile *right_tile)
    : left_source_(&left_tile->GetPositionLists()),
      right_source_(&right_tile->GetPositionLists()) {
  // Compute the output logical tile column count
  size_t left_tile_column_count = left_source_->size();
  size_t right_tile_column_count = right_source_->size();
  size_t output_tile_column_count =
      left_tile_column_count + right_tile_column_count;

  PELOTON_ASSERT(left_tile_column_count > 0);
  PELOTON_ASSERT(right_tile_column_count > 0);

  // Construct position lists for output tile
  for (size_t column_itr = 0; column_itr < output_tile_column_count;
       column_itr++) {
    output_lists_.push_back(std::vector<oid_t>());
  }
}

/**
 * @brief Set the schema of the tile.
 * @param ColumnInfo-based schema of the tile.
 */
void LogicalTile::SetSchema(std::vector<LogicalTile::ColumnInfo> &&schema) {
  schema_ = schema;
}

/**
 * @brief Adds column metadata to the logical tile.
 * @param base_tile Base tile that this column is from.
 * @param own_base_tile True if the logical tile should assume ownership of
 *                      the base tile passed in.
 * @param origin_column_id Original column id of this column in its base tile.
 * @param position_list_idx Index of the position list corresponding to this
 *        column.
 *
 * The position list corresponding to this column should be added
 * before the metadata.
 */
void LogicalTile::AddColumn(const std::shared_ptr<storage::Tile> &base_tile,
                            oid_t origin_column_id, oid_t position_list_idx) {
  ColumnInfo cp;

  // Add a reference to the base tile
  cp.base_tile = base_tile;

  cp.origin_column_id = origin_column_id;
  cp.position_list_idx = position_list_idx;
  schema_.push_back(cp);
}

/**
 * @brief Add the column specified in column_ids to this logical tile.
 */
void LogicalTile::AddColumns(
    const std::shared_ptr<storage::TileGroup> &tile_group,
    const std::vector<oid_t> &column_ids) {
  if(tile_group == nullptr){
    size_t i=0;
    for (oid_t origin_column_id : column_ids) {
      column_ids_[i] = origin_column_id;
      i++;
    }
  }else{
    const int position_list_idx = 0;
    auto tile_group_layout = tile_group->GetLayout();
    for (oid_t origin_column_id : column_ids) {
      oid_t base_tile_offset, tile_column_id;

      tile_group_layout.LocateTileAndColumn(origin_column_id, base_tile_offset,
                                            tile_column_id);

      AddColumn(tile_group->GetTileReference(base_tile_offset), tile_column_id,
                position_list_idx);
    }
  }
}
void LogicalTile::AddColumns(
    const storage::TileGroup *tile_group,
    const std::vector<oid_t> &column_ids) {
  if(tile_group == nullptr){
    size_t i=0;
    for (oid_t origin_column_id : column_ids) {
      column_ids_[i] = origin_column_id;
      i++;
    }
  }else{
    const int position_list_idx = 0;
    auto tile_group_layout = tile_group->GetLayout();
    for (oid_t origin_column_id : column_ids) {
      oid_t base_tile_offset, tile_column_id;

      tile_group_layout.LocateTileAndColumn(origin_column_id, base_tile_offset,
                                            tile_column_id);

      AddColumn(tile_group->GetTileReference(base_tile_offset), tile_column_id,
                position_list_idx);
    }
  }
}
void LogicalTile::AddTableColumns(const oid_t table_id,
    const std::vector<oid_t> &column_ids, const oid_t database_id) {
  for (oid_t origin_column_id : column_ids) {
    column_ids_.push_back(origin_column_id);
  }
  table_id_ = table_id;
  database_id_ = database_id;
}
void LogicalTile::AddTableName(const std::string table_name){
  this->table_name_ = table_name;
}
/**
 * @brief Given the original column ids, reorganize the schema to conform the
 * new column_ids
 * column_ids is a vector of oid_t. Each column_id is the index into the
 * original table schema
 * schema_ is a vector of ColumnInfos. Each ColumnInfo represents a column in
 * the corresponding place in colum_ids.
 */
void LogicalTile::ProjectColumns(const std::vector<oid_t> &original_column_ids,
                                 const std::vector<oid_t> &column_ids) {
  std::vector<ColumnInfo> new_schema;
  for (auto id : column_ids) {
    auto ret =
        std::find(original_column_ids.begin(), original_column_ids.end(), id);
    PELOTON_ASSERT(ret != original_column_ids.end());
    new_schema.push_back(schema_[*ret]);
  }

  // remove references to base tiles from columns that are projected away
  schema_ = std::move(new_schema);
}

//where column_key = all or where column_key > ? and column_key < ?
std::vector<std::string> LogicalTile::GetGoogleKValsAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::vector<std::string>> columns_;
  std::vector<std::string> rows;
  if(visible_range_.size()>0){
    oid_t tuple_id_st = visible_range_[0].first;
    oid_t tuple_id_ed = visible_range_[1].first;
    //all is visibilty
    for (size_t i = 0; i < column_count; i++) {
      std::vector<std::string> column_tile;
      std::vector<std::vector<std::string>> column_tiles_ =
          storage::StorageManager::GetInstance()->GetGoogleTreeKValues(table_id_,column_ids_[i],tile_group_st_, tile_group_ed_);
      size_t tile_count = column_tiles_.size();

      if(tile_group_st_ == tile_group_ed_){
        for(size_t tp = (tuple_id_st+1); tp < tuple_id_ed; tp++){
          column_tile.push_back(column_tiles_[0][tp]);
        }
      }else{
        for(size_t tp = (tuple_id_st+1); tp < column_tiles_[0].size(); tp++){
          column_tile.push_back(column_tiles_[0][tp]);
        }
      }

      for (size_t tl = 1; tl < (tile_count-1); tl++) {
        size_t tile_tuple_count = column_tiles_[tl].size();
        for(size_t tp =0; tp < tile_tuple_count; tp++){
          column_tile.push_back(column_tiles_[tl][tp]);
        }
      }

      if(tile_count>1){
        for(size_t tp = 0; tp < (tuple_id_ed+1); tp++){
          column_tile.push_back(column_tiles_[tile_count-1][tp]);
        }
      }

      columns_.push_back(column_tile);
    }
  }else{
    //all is visibilty
    for (size_t i = 0; i < column_count; i++) {
      std::vector<std::string> column_tile;
      std::vector<std::vector<std::string>> column_tiles_ =
          storage::StorageManager::GetInstance()->GetGoogleTreeKValues(table_id_,column_ids_[i],tile_group_st_, tile_group_ed_);
      size_t tile_count = column_tiles_.size();
      for (size_t tl = 0; tl < tile_count; tl++) {
        size_t tile_tuple_count = column_tiles_[tl].size();
        for(size_t tp =0; tp < tile_tuple_count; tp++){
          column_tile.push_back(column_tiles_[tl][tp]);
        }
      }
      columns_.push_back(column_tile);
    }

  }

  //project by partition
  for(size_t t=0; t<total_tuples_; t++){
    for (size_t i = 0; i < column_count; i++) {
      std::string str =  columns_[i][t];
      rows.push_back(std::move(str));
    }
  }

  return rows;
}
//where column_key = ?
std::vector<std::string> LogicalTile::GetGoogleTupleAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::string> rows;
  oid_t tuple_id_st = visible_range_[0].first;

  for (size_t i = 0; i < column_count; i++) {
    std::vector<std::string> column_tiles_ =
        storage::StorageManager::GetInstance()->GetGoogleTreeKV(
            table_id_, column_ids_[i], tile_group_st_);
    // project by partition
    rows.push_back(column_tiles_[tuple_id_st]);
  }

  return rows;
}

std::vector<std::string> LogicalTile::GetMassKValsAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::vector<std::string>> columns_;
  std::vector<std::string> rows;
  if(visible_range_.size()>0){
    oid_t tuple_id_st = visible_range_[0].first;
    oid_t tuple_id_ed = visible_range_[1].first;
    //all is visibilty
    for (size_t i = 0; i < column_count; i++) {
      std::vector<std::string> column_tile;
      concurrency::TransactionContext *tx =
          new concurrency::TransactionContext(0,
                                              IsolationLevelType::INVALID,
                                              1);
      tx->SetEpochId(0);
      std::vector<std::vector<std::string>> column_tiles_ =
          storage::StorageManager::GetInstance()->GetMassBtreeKValues(tx,table_id_,column_ids_[i],tile_group_st_, tile_group_ed_);
      size_t tile_count = column_tiles_.size();

      if(tile_group_st_ == tile_group_ed_){
        for(size_t tp = (tuple_id_st+1); tp < tuple_id_ed; tp++){
          column_tile.push_back(column_tiles_[0][tp]);
        }
      }else{
        for(size_t tp = (tuple_id_st+1); tp < column_tiles_[0].size(); tp++){
          column_tile.push_back(column_tiles_[0][tp]);
        }
      }

      for (size_t tl = 1; tl < (tile_count-1); tl++) {
        size_t tile_tuple_count = column_tiles_[tl].size();
        for(size_t tp =0; tp < tile_tuple_count; tp++){
          column_tile.push_back(column_tiles_[tl][tp]);
        }
      }

      if(tile_count>1){
        for(size_t tp = 0; tp < (tuple_id_ed+1); tp++){
          column_tile.push_back(column_tiles_[tile_count-1][tp]);
        }
      }

      columns_.push_back(column_tile);
    }
  }else{
    //all is visibilty
    for (size_t i = 0; i < column_count; i++) {
      std::vector<std::string> column_tile;
      concurrency::TransactionContext *tx =
          new concurrency::TransactionContext(0,
                                              IsolationLevelType::INVALID,
                                              1);
      tx->SetEpochId(0);
      std::vector<std::vector<std::string>> column_tiles_ =
          storage::StorageManager::GetInstance()->GetMassBtreeKValues(tx,table_id_,column_ids_[i],tile_group_st_, tile_group_ed_);
      size_t tile_count = column_tiles_.size();
      for (size_t tl = 0; tl < tile_count; tl++) {
        size_t tile_tuple_count = column_tiles_[tl].size();
        for(size_t tp =0; tp < tile_tuple_count; tp++){
          column_tile.push_back(column_tiles_[tl][tp]);
        }
      }
      columns_.push_back(column_tile);
    }

  }

  //project by partition
  for(size_t t=0; t<total_tuples_; t++){
    for (size_t i = 0; i < column_count; i++) {
      std::string str =  columns_[i][t];
      rows.push_back(std::move(str));
    }
  }

  return rows;
}

std::vector<std::string> LogicalTile::GetMassTupleAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::string> rows;
  oid_t tuple_id_st = visible_range_[0].first;

  for (size_t i = 0; i < column_count; i++) {
    std::vector<std::string> column_tiles_ =
        storage::StorageManager::GetInstance()->GetMassBtreeTuple(
            table_id_, column_ids_[i], tile_group_st_);

    // project by partition
    rows.push_back(column_tiles_[tuple_id_st]);
  }

  return rows;
}
std::vector<std::string> LogicalTile::GetHopscotchKValuesAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::vector<std::string>> columns_;
  std::vector<std::string> rows;
  oid_t current_tile_group = tile_group_st_;
  if(visible_range_.size()>0) {
    oid_t tuple_id_st = visible_range_[0].first;
    oid_t tuple_id_ed = visible_range_[1].first;
    while (current_tile_group<=tile_group_ed_){
      std::vector<std::vector<std::string>> tile_columns ={};
      storage::HopscotchMapKey hop_map_key{table_id_,current_tile_group};
      storage::TileGroup *tile_group = storage::StorageManager::GetInstance()->GetHopscotchKValue(hop_map_key);
      for (size_t i = 0; i < column_count; i++) {
        storage::Tile *tile = tile_group->GetTile(column_ids_[i]);
        std::vector<std::string> tile_ = tile->GetBlock(tile->GetAllocatedTupleCount());
        // project by partition
        tile_columns.push_back(tile_);
      }

      if((current_tile_group == tile_group_st_) && (current_tile_group == tile_group_ed_)){
        for(size_t tp = tuple_id_st ; tp < tuple_id_ed && tp <tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }else if((current_tile_group == tile_group_st_) && (current_tile_group != tile_group_ed_)){
        for(size_t tp = tuple_id_st ; tp <tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }else if((current_tile_group != tile_group_st_) && (current_tile_group == tile_group_ed_)){
        for(size_t tp = 0 ; tp < tuple_id_ed && tp <tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }else{
        for(size_t tp = 0 ; tp < tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }

      current_tile_group++;
    }
  }else{
    while (current_tile_group<tile_group_ed_){
      std::vector<std::vector<std::string>> tile_columns ={};
      storage::HopscotchMapKey hop_map_key{table_id_,current_tile_group};
      storage::TileGroup *tile_group = storage::StorageManager::GetInstance()->GetHopscotchKValue(hop_map_key);
      for (size_t i = 0; i < column_count; i++) {
        storage::Tile *tile = tile_group->GetTile(column_ids_[i]);
        std::vector<std::string> tile_ = tile->GetBlock(tile->GetAllocatedTupleCount());
        // project by partition
        tile_columns.push_back(tile_);
      }
      for(size_t tp =0 ; tp < tile_columns[0].size(); ++tp){
        for (size_t c = 0; c < tile_columns.size(); ++c) {
          std::string str = tile_columns[c][tp];
          rows.push_back(std::move(str));
        }
      }
      current_tile_group++;
    }
  }

  return rows;
}
std::vector<std::string> LogicalTile::GetHopscotchKTupleAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::string> rows;
  oid_t tuple_id_st = visible_range_[0].first;

  storage::HopscotchMapKey hop_map_key{table_id_,tile_group_st_};
  storage::TileGroup *tile_group = storage::StorageManager::GetInstance()->GetHopscotchKValue(hop_map_key);

  for (size_t i = 0; i < column_count; i++) {
    storage::Tile *tile = tile_group->GetTile(column_ids_[i]);
    std::vector<std::string> tile_ = tile->GetBlock(tile->GetAllocatedTupleCount());
    // project by partition
    rows.push_back(tile_[tuple_id_st]);
  }

  return rows;
}
std::vector<std::string> LogicalTile::GetCuckooKValuesAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::vector<std::string>> columns_;
  std::vector<std::string> rows;
  oid_t current_tile_group = tile_group_st_;
  if(visible_range_.size()>0) {
    oid_t tuple_id_st = visible_range_[0].first;
    oid_t tuple_id_ed = visible_range_[1].first;
    while (current_tile_group<=tile_group_ed_){
      std::vector<std::vector<std::string>> tile_columns ={};
      storage::CuckooMapKey cuckoo_map_key{table_id_,current_tile_group};
      storage::TileGroup *tile_group = storage::StorageManager::GetInstance()->GetCuckooKValue(cuckoo_map_key);
      for (size_t i = 0; i < column_count; i++) {
        storage::Tile *tile = tile_group->GetTile(column_ids_[i]);
        std::vector<std::string> tile_ = tile->GetBlock(tile->GetAllocatedTupleCount());
        // project by partition
        tile_columns.push_back(tile_);
      }

      if((current_tile_group == tile_group_st_) && (current_tile_group == tile_group_ed_)){
        for(size_t tp = tuple_id_st ; tp < tuple_id_ed && tp <tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }else if((current_tile_group == tile_group_st_) && (current_tile_group != tile_group_ed_)){
        for(size_t tp = tuple_id_st ; tp <tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }else if((current_tile_group != tile_group_st_) && (current_tile_group == tile_group_ed_)){
        for(size_t tp = 0 ; tp < tuple_id_ed && tp <tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }else{
        for(size_t tp = 0 ; tp < tile_columns[0].size(); ++tp) {
          for (size_t c = 0; c < tile_columns.size(); ++c) {
            std::string str = tile_columns[c][tp];
            rows.push_back(std::move(str));
          }
        }
      }

      current_tile_group++;
    }
  }else{
    while (current_tile_group<tile_group_ed_){
      std::vector<std::vector<std::string>> tile_columns ={};
      storage::CuckooMapKey cuckoo_map_key{table_id_,current_tile_group};
      storage::TileGroup *tile_group = storage::StorageManager::GetInstance()->GetCuckooKValue(cuckoo_map_key);
      for (size_t i = 0; i < column_count; i++) {
        storage::Tile *tile = tile_group->GetTile(column_ids_[i]);
        std::vector<std::string> tile_ = tile->GetBlock(tile->GetAllocatedTupleCount());
        // project by partition
        tile_columns.push_back(tile_);
      }
      for(size_t tp =0 ; tp < tile_columns[0].size(); ++tp){
        for (size_t c = 0; c < tile_columns.size(); ++c) {
          std::string str = tile_columns[c][tp];
          rows.push_back(std::move(str));
        }
      }
      current_tile_group++;
    }
  }

  return rows;
}
std::vector<std::string> LogicalTile::GetCuckooKTupleAsStrings() {
  size_t column_count = column_ids_.size();
  std::vector<std::string> rows;
  oid_t tuple_id_st = visible_range_[0].first;

  storage::CuckooMapKey cuckoo_map_key{table_id_,tile_group_st_};
  storage::TileGroup *tile_group = storage::StorageManager::GetInstance()->GetCuckooKValue(cuckoo_map_key);

  for (size_t i = 0; i < column_count; i++) {
    storage::Tile *tile = tile_group->GetTile(column_ids_[i]);
    std::vector<std::string> tile_ = tile->GetBlock(tile->GetAllocatedTupleCount());
    // project by partition
    rows.push_back(tile_[tuple_id_st]);
  }

  return rows;
}
//std::vector<std::vector<std::string>> LogicalTile::GetTbbconcurrentKValuesAsStrings() {
//  std::vector<std::vector<std::string>> rows;
//  size_t column_count = column_ids_.size();
//  storage::TbbMapKey tbb_map_key{this->table_name_,this->table_name_, partition_offset_};
//  storage::Tile *tuples_ = storage::StorageManager::GetInstance()->GetTbbConcurrentKValue(tbb_map_key);
//  for(size_t tid=0;tid<tuples_->GetActiveTupleCount();tid++){
//    std::vector<std::string> row;
//    for (size_t cl = 0; cl < column_count; cl++) {
//      oid_t col_id_ = column_ids_[cl];
//      type::Value val_ = tuples_->GetValue(tid,col_id_);
////      if(tile_tuples_visible_[partition_offset_][tid] == true){
//      row.push_back(val_.ToString());
////      }
//    }
//    rows.push_back(row);
//  }
//
//  return rows;
//}
//std::vector<std::vector<std::string>> LogicalTile::GetTbbconcurrentKTupleAsStrings()  {
//  std::vector<std::vector<std::string>> rows;
//  size_t column_count = column_ids_.size();
//  storage::TbbMapKey tbb_map_key{this->table_name_,this->table_name_, partition_offset_};
//  storage::Tile *tuples_ = storage::StorageManager::GetInstance()->GetTbbConcurrentKValue(tbb_map_key);
//  oid_t tid = tile_tuple_visible_[1];
////  oid_t visible = tile_tuple_visible_[2];
//  std::vector<std::string> row;
//  for (size_t cl = 0; cl < column_count; cl++) {
//    oid_t col_id_ = column_ids_[cl];
//    type::Value val_ = tuples_->GetValue(tid,col_id_);
////      if(visible == true){
//    row.push_back(val_.ToString());
////      }
//  }
//  rows.push_back(row);
//  return rows;
//}
std::vector<std::vector<std::string>> LogicalTile::GetAllValuesAsStrings(
    const std::vector<int> &result_format, bool use_to_string_null) {
  std::vector<std::vector<std::string>> string_tile;
  for (oid_t tuple_itr = 0; tuple_itr < total_tuples_; tuple_itr++) {
    std::vector<std::string> row;
    std::string empty_string;
    if (visible_rows_[tuple_itr] == false) continue;
    for (oid_t column_itr = 0; column_itr < schema_.size(); column_itr++) {
      const LogicalTile::ColumnInfo &cp = schema_[column_itr];
      oid_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_itr];
      // get the value from the base physical tile
      type::Value val;
      if (base_tuple_id == NULL_OID) {
        val = type::ValueFactory::GetNullValueByType(
            cp.base_tile->GetSchema()->GetType(cp.origin_column_id));
      } else {
        val = cp.base_tile->GetValue(base_tuple_id, cp.origin_column_id);
      }

      // LM: I put varchar here because we don't need to do endian conversion
      // for them, and assuming binary and text for a varchar are the same.
      if (result_format[column_itr] == 0 ||
          cp.base_tile->GetSchema()->GetType(cp.origin_column_id) ==
              type::TypeId::VARCHAR) {
        // don't let to_string function decide what NULL value is
        if (use_to_string_null == false && val.IsNull() == true) {
          // materialize Null values as 0B string
          row.push_back(empty_string);
        } else {
          // otherwise, materialize using ToString
          row.push_back(val.ToString());
        }
      } else {
        auto data_length =
            cp.base_tile->GetSchema()->GetLength(cp.origin_column_id);
        LOG_TRACE("data length: %ld", data_length);
        char *val_binary = new char[data_length];
        bool is_inlined = false;

        val.SerializeTo(val_binary, is_inlined, nullptr);

        // convert little endian to big endian...
        // TODO: This is stupid.... But I think this hack is fine for now.
        for (size_t i = 0; i < (data_length >> 1); ++i) {
          auto tmp_char = val_binary[i];
          val_binary[i] = val_binary[data_length - i - 1];
          val_binary[data_length - i - 1] = tmp_char;
        }

        row.push_back(std::string(val_binary, data_length));
      }
    }
    string_tile.push_back(row);
  }
  return string_tile;
}

const std::string LogicalTile::GetInfo() const {
  std::ostringstream os;
  os << "LOGICAL TILE [TotalTuples=" << total_tuples_ << "]" << std::endl;

  // for each row in the logical tile
  for (oid_t tuple_itr = 0; tuple_itr < total_tuples_; tuple_itr++) {
    if (visible_rows_[tuple_itr] == false) continue;

    for (oid_t column_itr = 0; column_itr < schema_.size(); column_itr++) {
      const LogicalTile::ColumnInfo &cp = schema_[column_itr];

      oid_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_itr];
      // get the value from the base physical tile
      if (base_tuple_id == NULL_OID) {
        type::Value value = type::ValueFactory::GetNullValueByType(
            cp.base_tile->GetSchema()->GetType(cp.origin_column_id));
        os << value.GetInfo() << " ";
      } else {
        type::Value value =
            (cp.base_tile->GetValue(base_tuple_id, cp.origin_column_id));
        os << value.GetInfo() << " ";
      }
    }
    os << std::endl;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

/**
 * @brief Generates map from each base tile to columns originally from that
 *        base tile to be materialized.
 * @param column_ids Ids of columns to be materialized.
 * @param source_tile Logical tile that contains mapping from columns to
 *        base tiles.
 * @param tile_to_cols Map to be populated with mappings from tile to columns.
 *
 * We generate this mapping so that we can materialize columns tile by tile for
 * efficiency reasons.
 */
void LogicalTile::GenerateTileToColMap(
    const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
    std::unordered_map<storage::Tile *, std::vector<oid_t>>
        &cols_in_physical_tile) {
  for (const auto &kv : old_to_new_cols) {
    oid_t col = kv.first;

    // figure out base physical tile for column in logical tile
    storage::Tile *base_tile = GetBaseTile(col);

    std::vector<oid_t> &cols_from_tile = cols_in_physical_tile[base_tile];
    cols_from_tile.push_back(col);
  }
}

/**
 * @brief Does the actual copying of data into the new physical tile.
 * @param source_tile Source tile to copy data from.
 * @param tile_to_cols Map from base tile to columns in that tile
 *        to be materialized.
 * @param dest_tile New tile to copy data into.
 */
void LogicalTile::MaterializeByTiles(
    const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
    const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
    storage::Tile *dest_tile,
    const peloton::LayoutType peloton_layout_mode) {
  bool row_wise_materialization = true;

  if (peloton_layout_mode == LayoutType::COLUMN)
    row_wise_materialization = false;

  // TODO: Make this a parameter
  auto dest_tile_column_count = dest_tile->GetColumnCount();
  oid_t column_count_threshold = 20;
  if (peloton_layout_mode == LayoutType::HYBRID &&
      dest_tile_column_count <= column_count_threshold)
    row_wise_materialization = false;

  // Materialize as needed
  if (row_wise_materialization == true) {
    MaterializeRowAtAtATime(old_to_new_cols, tile_to_cols, dest_tile);
  } else {
    MaterializeColumnAtATime(old_to_new_cols, tile_to_cols, dest_tile);
  }
}

void LogicalTile::MaterializeRowAtAtATime(
    const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
    const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
    storage::Tile *dest_tile) {
  ///////////////////////////
  // EACH PHYSICAL TILE
  ///////////////////////////
  // Copy over all data from each base tile.
  for (const auto &kv : tile_to_cols) {
    const std::vector<oid_t> &old_column_ids = kv.second;

    auto &schema = GetSchema();
    oid_t new_tuple_id = 0;

    auto &column_position_lists = GetPositionLists();

    // Get old column information
    std::vector<oid_t> old_column_position_idxs;
    std::vector<size_t> old_column_offsets;
    std::vector<type::TypeId> old_column_types;
    std::vector<bool> old_is_inlineds;
    std::vector<storage::Tile *> old_tiles;

    // Get new column information
    std::vector<size_t> new_column_offsets;
    std::vector<bool> new_is_inlineds;
    std::vector<size_t> new_column_lengths;

    // Amortize schema lookups once per column
    for (oid_t old_col_id : old_column_ids) {
      auto &column_info = schema[old_col_id];

      // Get the position list
      old_column_position_idxs.push_back(column_info.position_list_idx);

      // Get old column information
      storage::Tile *old_tile = column_info.base_tile.get();
      old_tiles.push_back(old_tile);
      auto old_schema = old_tile->GetSchema();
      oid_t old_column_id = column_info.origin_column_id;
      const size_t old_column_offset = old_schema->GetOffset(old_column_id);
      old_column_offsets.push_back(old_column_offset);
      const type::TypeId old_column_type =
          old_schema->GetType(old_column_id);
      old_column_types.push_back(old_column_type);
      const bool old_is_inlined = old_schema->IsInlined(old_column_id);
      old_is_inlineds.push_back(old_is_inlined);

      // Old to new column mapping
      auto it = old_to_new_cols.find(old_col_id);
      PELOTON_ASSERT(it != old_to_new_cols.end());

      // Get new column information
      oid_t new_column_id = it->second;
      auto new_schema = dest_tile->GetSchema();
      const size_t new_column_offset = new_schema->GetOffset(new_column_id);
      new_column_offsets.push_back(new_column_offset);
      const bool new_is_inlined = new_schema->IsInlined(new_column_id);
      new_is_inlineds.push_back(new_is_inlined);
      const size_t new_column_length =
          new_schema->GetAppropriateLength(new_column_id);
      new_column_lengths.push_back(new_column_length);
    }

    PELOTON_ASSERT(new_column_offsets.size() == old_column_ids.size());

    ///////////////////////////
    // EACH TUPLE
    ///////////////////////////
    // Copy all values in the tuple to the physical tile
    // This uses fast getter and setter functions
    for (oid_t old_tuple_id : *this) {
      ///////////////////////////
      // EACH COLUMN
      ///////////////////////////
      // Go over each column in given base physical tile
      oid_t col_itr = 0;

      for (oid_t old_col_id : old_column_position_idxs) {
        auto &column_position_list = column_position_lists[old_col_id];

        oid_t base_tuple_id = column_position_list[old_tuple_id];

        auto value = old_tiles[col_itr]->GetValueFast(
            base_tuple_id, old_column_offsets[col_itr],
            old_column_types[col_itr], old_is_inlineds[col_itr]);

        LOG_TRACE("Old Tuple : %u Column : %u ", old_tuple_id, old_col_id);
        LOG_TRACE("New Tuple : %u Column : %lu ", new_tuple_id,
                  new_column_offsets[col_itr]);

        dest_tile->SetValueFast(
            value, new_tuple_id, new_column_offsets[col_itr],
            new_is_inlineds[col_itr], new_column_lengths[col_itr]);

        // Go to next column
        col_itr++;
      }

      // Go to next tuple
      new_tuple_id++;
    }
  }
}

void LogicalTile::MaterializeColumnAtATime(
    const std::unordered_map<oid_t, oid_t> &old_to_new_cols,
    const std::unordered_map<storage::Tile *, std::vector<oid_t>> &tile_to_cols,
    storage::Tile *dest_tile) {
  ///////////////////////////
  // EACH PHYSICAL TILE
  ///////////////////////////
  // Copy over all data from each base tile.
  for (const auto &kv : tile_to_cols) {
    const std::vector<oid_t> &old_column_ids = kv.second;

    ///////////////////////////
    // EACH COLUMN
    ///////////////////////////
    // Go over each column in given base physical tile
    for (oid_t old_col_id : old_column_ids) {
      auto &column_info = GetColumnInfo(old_col_id);

      // Amortize schema lookups once per column
      storage::Tile *old_tile = column_info.base_tile.get();
      auto old_schema = old_tile->GetSchema();

      // Get old column information
      oid_t old_column_id = column_info.origin_column_id;
      const size_t old_column_offset = old_schema->GetOffset(old_column_id);
      const type::TypeId old_column_type =
          old_schema->GetType(old_column_id);
      const bool old_is_inlined = old_schema->IsInlined(old_column_id);

      // Old to new column mapping
      auto it = old_to_new_cols.find(old_col_id);
      PELOTON_ASSERT(it != old_to_new_cols.end());

      // Get new column information
      oid_t new_column_id = it->second;
      auto new_schema = dest_tile->GetSchema();
      const size_t new_column_offset = new_schema->GetOffset(new_column_id);
      const bool new_is_inlined = new_schema->IsInlined(new_column_id);
      const size_t new_column_length =
          new_schema->GetAppropriateLength(new_column_id);

      // Get the position list
      auto &column_position_list =
          GetPositionList(column_info.position_list_idx);
      oid_t new_tuple_id = 0;

      // Copy all values in the column to the physical tile
      // This uses fast getter and setter functions
      ///////////////////////////
      // EACH TUPLE
      ///////////////////////////
      for (oid_t old_tuple_id : *this) {
        oid_t base_tuple_id = column_position_list[old_tuple_id];
        type::Value value = (old_tile->GetValueFast(
            base_tuple_id, old_column_offset, old_column_type, old_is_inlined));

        LOG_TRACE("Old Tuple : %u Column : %u ", old_tuple_id, old_col_id);
        LOG_TRACE("New Tuple : %u Column : %u ", new_tuple_id, new_column_id);

        dest_tile->SetValueFast(value, new_tuple_id, new_column_offset,
                                new_is_inlined, new_column_length);

        // Go to next tuple
        new_tuple_id++;
      }
    }
  }
}

/**
 * @brief Create a physical tile
 * @param
 * @return Physical tile
 */
std::unique_ptr<storage::Tile> LogicalTile::Materialize() {
  // Create new schema according underlying physical tile
  std::unique_ptr<catalog::Schema> source_tile_schema(GetPhysicalSchema());

  // Get the number of tuples within this logical tiles
  const int num_tuples = GetTupleCount();

  // const catalog::Schema *output_schema;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t column_count = source_tile_schema->GetColumnCount();
  for (oid_t col = 0; col < column_count; col++) {
    old_to_new_cols[col] = col;
  }

  // Generate mappings.
  std::unordered_map<storage::Tile *, std::vector<oid_t>> tile_to_cols;
  GenerateTileToColMap(old_to_new_cols, tile_to_cols);

  // Create new physical tile.
  std::unique_ptr<storage::Tile> dest_tile(
      storage::TileFactory::GetTempTile(*source_tile_schema, num_tuples));

  // Proceed to materialize logical tile by physical tile at a time.
  MaterializeByTiles(old_to_new_cols, tile_to_cols, dest_tile.get());

  // Wrap physical tile in logical tile.
  return dest_tile;
}

}  // namespace executor
}  // namespace peloton
