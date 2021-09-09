//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.cpp
//
// Identification: src/storage/data_table.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/layout_catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "common/container_tuple.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "gc/gc_manager_factory.h"
#include "index/index.h"
#include "logging/log_manager.h"
#include "storage/abstract_table.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"
#include "tuning/clusterer.h"
#include "tuning/sample.h"
#include "masstree/encoder.h"
#include "type/type_id.h"
#include "climits"
#include "index/compact_ints_key.h"


//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

std::vector<peloton::oid_t> sdbench_column_ids;

double peloton_projectivity;

int peloton_num_groups;

namespace peloton {
namespace storage {

oid_t DataTable::invalid_tile_group_id = -1;

size_t DataTable::default_active_tilegroup_count_ = 1;
size_t DataTable::default_active_indirection_array_count_ = 1;

DataTable::DataTable(catalog::Schema *schema, const std::string &table_name,
                     const oid_t &database_oid, const oid_t &table_oid,
                     const size_t &tuples_per_tilegroup, const bool own_schema,
                     const bool adapt_table, const bool is_catalog,
                     const peloton::LayoutType layout_type)
    : AbstractTable(table_oid, schema, own_schema, layout_type),
      database_oid(database_oid),
      table_name(table_name),
      tuples_per_tilegroup_(tuples_per_tilegroup),
      current_layout_oid_(ATOMIC_VAR_INIT(COLUMN_STORE_LAYOUT_OID)),
      adapt_table_(adapt_table),
      trigger_list_(new trigger::TriggerList()) {
  if (is_catalog == true) {
    active_tilegroup_count_ = 1;
    active_indirection_array_count_ = 1;
    is_catalog_ = true;
  } else {
    active_tilegroup_count_ = default_active_tilegroup_count_;
    active_indirection_array_count_ = default_active_indirection_array_count_;
  }

  active_tile_groups_.resize(active_tilegroup_count_);

  active_indirection_arrays_.resize(active_indirection_array_count_);
  // Create tile groups.
  for (size_t i = 0; i < active_tilegroup_count_; ++i) {
    AddDefaultTileGroup(i);
  }

  // Create indirection layers.
  for (size_t i = 0; i < active_indirection_array_count_; ++i) {
    AddDefaultIndirectionArray(i);
  }

}

DataTable::~DataTable() {
  // clean up tile groups by dropping the references in the catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      LOG_TRACE("Dropping tile group : %u ", tile_group_id);
      // drop tile group in catalog
      storage_manager->DropTileGroup(tile_group_id);
    }
  }

  // drop all indirection arrays
  for (auto indirection_array : active_indirection_arrays_) {
    auto oid = indirection_array->GetOid();
    catalog_manager.DropIndirectionArray(oid);
  }
  // AbstractTable cleans up the schema
}

//===--------------------------------------------------------------------===//
// TUPLE HELPER OPERATIONS
//===--------------------------------------------------------------------===//

bool DataTable::CheckNotNulls(const AbstractTuple *tuple,
                              oid_t column_idx) const {
  if (tuple->GetValue(column_idx).IsNull()) {
    LOG_TRACE(
        "%u th attribute in the tuple was NULL. It is non-nullable "
        "attribute.",
        column_idx);
    return false;
  }
  return true;
}

bool DataTable::CheckConstraints(const AbstractTuple *tuple) const {
  // make sure that the given tuple does not violate constraints.

  // NOT NULL constraint
  for (oid_t column_id : schema->GetNotNullColumns()) {
    if (schema->AllowNull(column_id) == false &&
        CheckNotNulls(tuple, column_id) == false) {
      std::string error =
          StringUtil::Format("NOT NULL constraint violated on column '%s' : %s",
                             schema->GetColumn(column_id).GetName().c_str(),
                             tuple->GetInfo().c_str());
      throw ConstraintException(error);
    }
  }

  // DEFAULT constraint should not be handled here
  // Handled in higher hierarchy

  // multi-column constraints
  for (auto cons_pair : schema->GetConstraints()) {
    auto cons = cons_pair.second;
    ConstraintType type = cons->GetType();
    switch (type) {
      case ConstraintType::CHECK: {
        //          std::pair<ExpressionType, type::Value> exp =
        //          cons.GetCheckExpression();
        //          if (CheckExp(tuple, column_itr, exp) == false) {
        //            LOG_TRACE("CHECK EXPRESSION constraint violated");
        //            throw ConstraintException(
        //                "CHECK EXPRESSION constraint violated : " +
        //                std::string(tuple->GetInfo()));
        //          }
        break;
      }
      case ConstraintType::UNIQUE: {
        break;
      }
      case ConstraintType::PRIMARY: {
        break;
      }
      case ConstraintType::FOREIGN: {
        break;
      }
      case ConstraintType::EXCLUSION: {
        break;
      }
      default: {
        std::string error =
            StringUtil::Format("ConstraintType '%s' is not supported",
                               ConstraintTypeToString(type).c_str());
        LOG_TRACE("%s", error.c_str());
        throw ConstraintException(error);
      }
    }  // SWITCH
  }    // FOR (constraints)

  return true;
}

// this function is called when update/delete/insert is performed.
// this function first checks whether there's available slot.
// if yes, then directly return the available slot.
// in particular, if this is the last slot, a new tile group is created.
// if there's no available slot, then some other threads must be allocating a
// new tile group.
// we just wait until a new tuple slot in the newly allocated tile group is
// available.
// when updating a tuple, we will invoke this function with the argument set to
// nullptr.
// this is because we want to minimize data copy overhead by performing
// in-place update at executor level.
// however, when performing insert, we have to copy data immediately,
// and the argument cannot be set to nullptr.
ItemPointer DataTable::GetEmptyTupleSlot(const storage::Tuple *tuple) {
  //=============== garbage collection==================
  // check if there are recycled tuple slots
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // when inserting a tuple
    if (tuple != nullptr) {
      auto tile_group = storage::StorageManager::GetInstance()->GetTileGroup(
          free_item_pointer.block);
      tile_group->CopyTuple(tuple, free_item_pointer.offset);
    }
    return free_item_pointer;
  }
  //====================================================
  // active_tile_group_id is the index of the array(active_tile_groups)
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  std::shared_ptr<storage::TileGroup> tile_group;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_id = INVALID_OID;

  // get valid tuple.
  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];

    tuple_slot = tile_group->InsertTuple(tuple);

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }
  }

  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup(active_tile_group_id);
  }

  LOG_TRACE("tile group count: %lu, tile group id: %u, address: %p",
            tile_group_count_.load(), tile_group->GetTileGroupId(),
            tile_group.get());
  type::Value tp_0 = tile_group->GetValue(0,0);
  type::Value tp_slot= tuple->GetValue(0);
  if(storage::StorageManager::GetInstance()->GetDatabaseWithOid(database_oid)->GetDBName()=="default_database"){
    if(this->sparse_index.Contains((oid_t)tile_group_count_) == true ){
      this->sparse_index.Update((oid_t)tile_group_count_,std::make_pair(tp_0,tp_slot) );
    }else{
      this->sparse_index.Insert((oid_t)tile_group_count_,std::make_pair(tp_0,tp_slot) );
    }
  }

  // Set tuple location
  ItemPointer location(tile_group_id, tuple_slot);

  location.SetLocation(tile_group.get());

  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
ItemPointer DataTable::InsertEmptyVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

ItemPointer DataTable::AcquireVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

bool DataTable::InstallVersion(const AbstractTuple *tuple,
                               const TargetList *targets_ptr,
                               concurrency::TransactionContext *transaction,
                               ItemPointer *index_entry_ptr) {
  if (CheckConstraints(tuple) == false) {
    LOG_TRACE("InsertVersion(): Constraint violated");
    return false;
  }

  // Index checks and updates
  if (InsertInSecondaryIndexes(tuple, targets_ptr, transaction,
                               index_entry_ptr) == false) {
    LOG_TRACE("Index constraint violated");
    return false;
  }
  return true;
}

ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple,
                                   concurrency::TransactionContext *transaction,
                                   ItemPointer **index_entry_ptr,
                                   bool check_fk) {
  type::Value c0 = tuple->GetValue(0);
  if(c0.GetTypeId() == type::TypeId::INTEGER){
    int32_t max_= std::numeric_limits<int32_t>::max() - 1;
    if(c0.CompareEquals(type::ValueFactory::GetIntegerValue(max_))==CmpBool::CmpTrue){
      LOG_DEBUG("start kev store");
      for(size_t i=0;i<this->tile_group_count_;i++){
        KVStoreTileGroup(i);
      }
      return INVALID_ITEMPOINTER;
    }
  }

  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  auto result =
      InsertTuple(tuple, location, transaction, index_entry_ptr, check_fk);
  if (result == false) {
    return INVALID_ITEMPOINTER;
  }

  return location;
}

void DataTable::KVStoreTileGroup(const oid_t &tile_group_offset){
  // First, check if the tile group is in this table
  if (tile_group_offset >= tile_groups_.GetSize()) {
    LOG_ERROR("Tile group offset not found in table : %u ", tile_group_offset);
  }

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);
//  LOG_DEBUG("358 line tile group id,%uz",tile_group_id);
  // Get orig tile group from catalog
  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_group = storage_manager->GetTileGroup(tile_group_id);
  oid_t table_id = tile_group->GetTableId();
  std::vector<catalog::Column> column_info = tile_group->GetTile(0)->GetSchema()->GetColumns();
  oid_t tuple_count = tile_group->GetNextTupleSlot();
  Tile *org_tile = tile_group->GetTile(0);


  if(tuple_count>0){
//    //==========================googletree===========================
//    oid_t tile_id = storage_manager->GetNextTileId();
//    for (oid_t column_itr = 0; column_itr < column_info.size(); column_itr++) {
//      catalog::Schema *tile_schema(new catalog::Schema({column_info[column_itr]}));
//      std::unique_ptr<Tile> tile_g(storage::TileFactory::GetTile(
//          BackendType::MM, tile_group->GetDatabaseId(), table_id,
//          tile_group_id, tile_id, tile_group->GetHeader(), *tile_schema,
//          tile_group.get(), tuple_count));
//
//      for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
//        type::Value val = (org_tile->GetValue(tuple_itr, column_itr));
//        tile_g->SetValue(val, tuple_itr, 0);
//      }
//
//      //add to google_B-tree
//      index::CompactIntsKey<2> key_g;
//      key_g.AddInteger(table_id,0);
//      key_g.AddInteger(column_itr,sizeof(table_id));
//      key_g.AddInteger(tile_group_offset,(sizeof(table_id)+sizeof(column_itr)));
//      storage_manager->AddToGoogleBtree(key_g,tile_g.release());
//    }

// //=================================masstree=============================
//    oid_t tile_id = storage_manager->GetNextTileId();
//    for (oid_t column_itr = 0; column_itr < column_info.size(); column_itr++) {
//      catalog::Schema *tile_schema(new catalog::Schema({column_info[column_itr]}));
//      std::unique_ptr<Tile> tile_m(storage::TileFactory::GetTile(
//          BackendType::MM, tile_group->GetDatabaseId(), table_id,
//          tile_group_id, tile_id, tile_group->GetHeader(), *tile_schema,
//          tile_group.get(), tuple_count));
//
//      for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
//        type::Value val = (org_tile->GetValue(tuple_itr, column_itr));
//        tile_m->SetValue(val, tuple_itr, 0);
//      }
//
//      // add to mass_B+tree
//      index::CompactIntsKey<2> key;
//      key.AddInteger(table_id, 0);
//      key.AddInteger(column_itr, sizeof(table_id));
//      key.AddInteger(tile_group_offset, (sizeof(table_id) + sizeof(column_itr)));
//      concurrency::TransactionContext *tx =
//          new concurrency::TransactionContext(0, IsolationLevelType::INVALID, 1);
//      tx->SetEpochId(0);
//      concurrency::TransactionContext &tr = *tx;
//      key.GetInfo();
//      varstr *var_ = new varstr(key.GetRawData(), key.key_size_byte);
//      varstr &varstr_ = *var_;
//      storage_manager->AddToMassBtree(tr, varstr_, tile_m.release());
//    }

    //add to bwtree
    //key:table_id,column_id,partition_id
    //value:storage::Tile *
//    catalog::Column column0(type::TypeId::INTEGER, 4, "table_id", true);
//    catalog::Column column1(type::TypeId::INTEGER, 4, "column_id", true);
//    catalog::Column column2(type::TypeId::INTEGER, 4, "partition_id", true);
//    catalog::Schema *key_schema(new catalog::Schema({column0,column1,column2}));
//    std::unique_ptr<storage::Tuple> index_key(new storage::Tuple(key_schema, true));
//    auto value_0 = type::ValueFactory::GetIntegerValue(table_id).Copy();
//    auto value_1 = type::ValueFactory::GetIntegerValue(column_itr).Copy();
//    auto value_2 = type::ValueFactory::GetIntegerValue(tile_group_offset).Copy();
//    index_key->SetValue(0, value_0, nullptr);
//    index_key->SetValue(1, value_1, nullptr);
//    index_key->SetValue(2, value_2, nullptr);
//  LOG_DEBUG("insert tree key%s", index_key->GetInfo().c_str());

    //item=block_location
//    ItemPointer *index_entry_ptr_=new ItemPointer();
//    index_entry_ptr_->SetTileLocation(tile_b.release());

//    storage_manager->AddToBwBtree(index_key.get(), index_entry_ptr_);

// =============================Map=====================================
    std::vector<catalog::Schema> new_schema;
    std::map<oid_t, std::pair<oid_t, oid_t>> column_map;
    for (oid_t col_id = 0; col_id < column_info.size(); col_id++) {
      catalog::Schema tile_schema({column_info[col_id]});
      new_schema.push_back(tile_schema);
      column_map[col_id] = std::make_pair(col_id, 0);
    }
    std::shared_ptr<const storage::Layout> new_layout =
        std::make_shared<const storage::Layout>(column_map);
//   //=================================cuckoomap================================
//    std::unique_ptr<storage::TileGroup> new_tile_group_c(
//    TileGroupFactory::GetTileGroup(tile_group->GetDatabaseId(), table_id,
//                                   tile_group->GetTileGroupId(),
//                                   tile_group->GetAbstractTable(),
//                                   new_schema, new_layout, tuple_count));
//    // Go over each column copying onto the new tile group
//    for (oid_t column_itr = 0; column_itr < column_info.size(); column_itr++) {
//      // Locate the original base tile and tile column offset
//      auto new_tile_h = new_tile_group_c->GetTile(column_itr);
//      // Copy the column over to the new tile group
//      for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
//        type::Value val = (org_tile->GetValue(tuple_itr, column_itr));
//        new_tile_h->SetValue(val, tuple_itr, 0);
//      }
//    }
//
//    //  storage::Tile *tile_hop = tile_group->GetTile(0);
//    storage::CuckooMapKey cuckoo_map{table_oid, tile_group_offset};
//    storage::StorageManager::GetInstance()->AddToCuckooMap(
//        cuckoo_map, new_tile_group_c.release());

// ================================hopscotchmap============================
  std::unique_ptr<storage::TileGroup> new_tile_group_h(
      TileGroupFactory::GetTileGroup(tile_group->GetDatabaseId(), table_id,
                                     tile_group->GetTileGroupId(),
                                     tile_group->GetAbstractTable(),
                                     new_schema, new_layout, tuple_count));
  // Go over each column copying onto the new tile group
  for (oid_t column_itr = 0; column_itr < column_info.size(); column_itr++) {
    // Locate the original base tile and tile column offset
    auto new_tile_h = new_tile_group_h->GetTile(column_itr);
    // Copy the column over to the new tile group
    for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
      type::Value val = (org_tile->GetValue(tuple_itr, column_itr));
      new_tile_h->SetValue(val, tuple_itr, 0);
    }
  }

  //  storage::Tile *tile_hop = tile_group->GetTile(0);
  storage::HopscotchMapKey hopscotch_map{table_oid, tile_group_offset};
  storage::StorageManager::GetInstance()->AddToHopscotchMap(
      hopscotch_map, new_tile_group_h.release());

  }
}

//void DataTable::InsertTupleToMap(oid_t partition_offset,std::vector<std::vector<type::Value>> tuples){
  //add tuples to the hopscotch hash map
  // column_id[0] is the key
//  storage::HopscotchMapKey hopscotch_map{table_oid, partition_offset};
//  storage::StorageManager::GetInstance()->AddToHopscotchMap(hopscotch_map, std::move(tuples));
////  LOG_DEBUG("key: partition_id %s, tuple_key %s, value: location %p, size %zu",partition_id.ToString().c_str(),tuple_key.ToString().c_str(),
////            &tuple_vec_h,tuple_vec_h.size());
//
//  //add tuples to the tbb concurrent hash map
//  //type::Value key_;
//  type::Value tuple_key_ = type::ValueFactory::GetIntegerValue(0);
//  type::Value hash_key_ = type::ValueFactory::GetIntegerValue(0);
//  std::vector<type::Value> tuple_vec_t;
//  oid_t column_count_ = tuple->GetColumnCount();
//  for(oid_t i = 0; i< column_count_; i++){
//    tuple_vec_t.push_back(tuple->GetValue(i)) ;
//  }
//  if((table_name == "region") || (table_name == "nation") || (table_name == "part") || (table_name == "supplier")){
//    tuple_key_ = tuple->GetValue(0);
//    hash_key_ = tuple->GetValue(0);
//    std::string hash_table_name = table_name;
//    TbbMapKey tbb_map_key{table_name,hash_table_name, tuple_key_, hash_key_};
//    storage::StorageManager::GetInstance()->AddToTbbConcurrentMap(tbb_map_key, std::move(tuple_vec_t));
//  }else if(table_name == "partsupp"){
//    //supplierkey
//    tuple_key_ = tuple->GetValue(1);
//    hash_key_ = tuple->GetValue(0);
//    std::string hash_table_name = "part";
//    TbbMapKey tbb_map_key_0{table_name, hash_table_name, tuple_key_, hash_key_};
//    storage::StorageManager::GetInstance()->AddToTbbConcurrentMap(tbb_map_key_0, std::move(tuple_vec_t));
//    //partkey
//    tuple_key_ = tuple->GetValue(0);
//    hash_key_ = tuple->GetValue(1);
//    hash_table_name = "supplier";
//    TbbMapKey tbb_map_key_1{table_name, hash_table_name, tuple_key_, hash_key_};
//    storage::StorageManager::GetInstance()->AddToTbbConcurrentMap(tbb_map_key_1, std::move(tuple_vec_t));
//  }else if(table_name == "customer"){
//    //customerkey
//    tuple_key_ = tuple->GetValue(0);
//    hash_key_ = tuple->GetValue(0);
//    std::string hash_table_name = table_name;
//    TbbMapKey tbb_map_key{table_name, hash_table_name, tuple_key_, hash_key_};
//    storage::StorageManager::GetInstance()->AddToTbbConcurrentMap(tbb_map_key, std::move(tuple_vec_t));
//  }else if(table_name == "orders"){
//    //orderKey
//    tuple_key_ = tuple->GetValue(0);
//    //customerKey
//    hash_key_ = tuple->GetValue(1);
//    std::string hash_table_name = "customer";
//    TbbMapKey tbb_map_key{table_name, hash_table_name, tuple_key_, hash_key_};
//    storage::StorageManager::GetInstance()->AddToTbbConcurrentMap(tbb_map_key, std::move(tuple_vec_t));
//  }else if(table_name == "lineitem"){
//    //orderKey
//    tuple_key_ = tuple->GetValue(0);
//    //partition_id, shipdate(year)
//    hash_key_ = type::ValueFactory::GetVarcharValue(tuple->GetValue(10).ToString().substr(0,4));
//    hash_key_.CastAs(type::TypeId::INTEGER);
//    std::string hash_table_name = table_name;
//    TbbMapKey tbb_map_key{table_name, hash_table_name, tuple_key_, hash_key_};
//    storage::StorageManager::GetInstance()->AddToTbbConcurrentMap(tbb_map_key, std::move(tuple_vec_t));
//  }else{
//    tuple_key_ = tuple->GetValue(0);
//    hash_key_ = tuple->GetValue(0).Divide(type::ValueFactory::GetIntegerValue(TEST_TUPLES_PER_TILEGROUP));
//    std::string hash_table_name = table_name;
//    TbbMapKey tbb_map_key{table_name, hash_table_name, tuple_key_, hash_key_};
//    storage::StorageManager::GetInstance()->AddToTbbConcurrentMap(tbb_map_key, std::move(tuple_vec_t));
//    LOG_DEBUG("key: partition_id %s, tuple_key %s, value: location %p, size %zu",hash_key_.ToString().c_str(),tuple_key_.ToString().c_str(),
////              &tuple_vec_t,tuple_vec_t.size());
//  }
//
//}

bool DataTable::InsertTuple(const AbstractTuple *tuple, ItemPointer location,
                            concurrency::TransactionContext *transaction,
                            ItemPointer **index_entry_ptr, bool check_fk) {
  if (CheckConstraints(tuple) == false) {
    LOG_TRACE("InsertTuple(): Constraint violated");
    return false;
  }

  // the upper layer may not pass a index_entry_ptr (default value: nullptr)
  // into the function.
  // in this case, we have to create a temp_ptr to hold the content.
  ItemPointer *temp_ptr = nullptr;
  if (index_entry_ptr == nullptr) {
    index_entry_ptr = &temp_ptr;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  auto index_count = GetIndexCount();
  if (index_count == 0) {
    if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
      LOG_TRACE("ForeignKey constraint violated");
      return false;
    }
    IncreaseTupleCount(1);
    return true;
  }
  // Index checks and updates
  if (InsertInIndexes(tuple, location, transaction, index_entry_ptr) == false) {
    LOG_TRACE("Index constraint violated");
    return false;
  }

  // ForeignKey checks
  if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
    LOG_TRACE("ForeignKey constraint violated");
    return false;
  }

  PELOTON_ASSERT((*index_entry_ptr)->block == location.block &&
                 (*index_entry_ptr)->offset == location.offset);

  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return true;
}

// insert tuple into a table that is without index.
ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  UNUSED_ATTRIBUTE auto index_count = GetIndexCount();
  PELOTON_ASSERT(index_count == 0);
  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return location;
}

/**
 * @brief Insert a tuple into all indexes. If index is primary/unique,
 * check visibility of existing
 * index entries.
 * @warning This still doesn't guarantee serializability.
 *
 * @returns True on success, false if a visible entry exists (in case of
 *primary/unique).
 */
bool DataTable::InsertInIndexes(const AbstractTuple *tuple,
                                ItemPointer location,
                                concurrency::TransactionContext *transaction,
                                ItemPointer **index_entry_ptr) {
  int index_count = GetIndexCount();

  size_t active_indirection_array_id =
      number_of_tuples_ % active_indirection_array_count_;

  size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

  while (true) {
    auto active_indirection_array =
        active_indirection_arrays_[active_indirection_array_id];
    indirection_offset = active_indirection_array->AllocateIndirection();

    if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
      *index_entry_ptr =
          active_indirection_array->GetIndirectionByOffset(indirection_offset);
      break;
    }
  }

  (*index_entry_ptr)->block = location.block;
  (*index_entry_ptr)->offset = location.offset;

  if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
    AddDefaultIndirectionArray(active_indirection_array_id);
  }

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Since this is NOT protected by a lock, concurrent insert may happen.
  bool res = true;
  int success_count = 0;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        // get unique tuple from primary/unique index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        res = index->CondInsertEntry(key.get(), *index_entry_ptr, fn);
      } break;

      case IndexConstraintType::DEFAULT:
      default:
        index->InsertEntry(key.get(), *index_entry_ptr);
        break;
    }

    // Handle failure
    if (res == false) {
      // If some of the indexes have been inserted,
      // the pointer has a chance to be dereferenced by readers and it cannot be
      // deleted
      *index_entry_ptr = nullptr;
      return false;
    } else {
      success_count += 1;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }

  return true;
}

bool DataTable::InsertInSecondaryIndexes(
    const AbstractTuple *tuple, const TargetList *targets_ptr,
    concurrency::TransactionContext *transaction,
    ItemPointer *index_entry_ptr) {
  int index_count = GetIndexCount();
  // Transform the target list into a hash set
  // when attempting to perform insertion to a secondary index,
  // we must check whether the updated column is a secondary index column.
  // insertion happens only if the updated column is a secondary index column.
  std::unordered_set<oid_t> targets_set;
  for (auto target : *targets_ptr) {
    targets_set.insert(target.first);
  }

  bool res = true;

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Check existence for primary/unique indexes
  // Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
      continue;
    }

    // Check if we need to update the secondary index
    bool updated = false;
    for (auto col : indexed_columns) {
      if (targets_set.find(col) != targets_set.end()) {
        updated = true;
        break;
      }
    }

    // If attributes on key are not updated, skip the index update
    if (updated == false) {
      continue;
    }

    // Key attributes are updated, insert a new entry in all secondary index
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));

    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        res = index->CondInsertEntry(key.get(), index_entry_ptr, fn);
      } break;
      case IndexConstraintType::DEFAULT:
      default:
        index->InsertEntry(key.get(), index_entry_ptr);
        break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
  return res;
}

/**
 * @brief This function checks any other table which has a foreign key
 *constraint
 * referencing the current table, where a tuple is updated/deleted. The final
 * result depends on the type of cascade action.
 *
 * @param prev_tuple: The tuple which will be updated/deleted in the current
 * table
 * @param new_tuple: The new tuple after update. This parameter is ignored
 * if is_update is false.
 * @param current_txn: The current transaction context
 * @param context: The executor context passed from upper level
 * @param is_update: whether this is a update action (false means delete)
 *
 * @return True if the check is successful (nothing happens) or the cascade
 *operation
 * is done properly. Otherwise returns false. Note that the transaction result
 * is not set in this function.
 */
bool DataTable::CheckForeignKeySrcAndCascade(
    storage::Tuple *prev_tuple, storage::Tuple *new_tuple,
    concurrency::TransactionContext *current_txn,
    executor::ExecutorContext *context, bool is_update) {
  if (!schema->HasForeignKeySources()) return true;

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  for (auto cons : schema->GetForeignKeySources()) {
    // Check if any row in the source table references the current tuple
    oid_t source_table_id = cons->GetTableOid();
    storage::DataTable *src_table = nullptr;
    try {
      src_table = (storage::DataTable *)storage::StorageManager::GetInstance()
                      ->GetTableWithOid(GetDatabaseOid(), source_table_id);
    } catch (CatalogException &e) {
      LOG_TRACE("Can't find table %d! Return false", source_table_id);
      return false;
    }

    int src_table_index_count = src_table->GetIndexCount();
    for (int iter = 0; iter < src_table_index_count; iter++) {
      auto index = src_table->GetIndex(iter);
      if (index == nullptr) continue;

      // Make sure this is the right index to search in
      if (index->GetOid() == cons->GetIndexOid() &&
          index->GetMetadata()->GetKeyAttrs() == cons->GetColumnIds()) {
        LOG_DEBUG("Searching in source tables's fk index...\n");

        std::vector<oid_t> key_attrs = cons->GetColumnIds();
        std::unique_ptr<catalog::Schema> fk_schema(
            catalog::Schema::CopySchema(src_table->GetSchema(), key_attrs));
        std::unique_ptr<storage::Tuple> key(
            new storage::Tuple(fk_schema.get(), true));

        key->SetFromTuple(prev_tuple, cons->GetFKSinkColumnIds(),
                          index->GetPool());

        std::vector<ItemPointer *> location_ptrs;
        index->ScanKey(key.get(), location_ptrs);

        if (location_ptrs.size() > 0) {
          LOG_DEBUG("Something found in the source table!\n");

          for (ItemPointer *ptr : location_ptrs) {
            auto src_tile_group = src_table->GetTileGroupById(ptr->block);
            auto src_tile_group_header = src_tile_group->GetHeader();

            auto visibility = transaction_manager.IsVisible(
                current_txn, src_tile_group_header, ptr->offset,
                VisibilityIdType::COMMIT_ID);

            if (visibility != VisibilityType::OK) continue;

            switch (cons->GetFKUpdateAction()) {
              // Currently NOACTION is the same as RESTRICT
              case FKConstrActionType::NOACTION:
              case FKConstrActionType::RESTRICT: {
                return false;
              }
              case FKConstrActionType::CASCADE:
              default: {
                // Update
                bool src_is_owner = transaction_manager.IsOwner(
                    current_txn, src_tile_group_header, ptr->offset);

                // Read the referencing tuple, update the read timestamp so that
                // we can
                // delete it later
                bool ret = transaction_manager.PerformRead(
                    current_txn, *ptr, src_tile_group_header, true);

                if (ret == false) {
                  if (src_is_owner) {
                    transaction_manager.YieldOwnership(
                        current_txn, src_tile_group_header, ptr->offset);
                  }
                  return false;
                }

                ContainerTuple<storage::TileGroup> src_old_tuple(
                    src_tile_group.get(), ptr->offset);
                storage::Tuple src_new_tuple(src_table->GetSchema(), true);

                if (is_update) {
                  for (oid_t col_itr = 0;
                       col_itr < src_table->GetSchema()->GetColumnCount();
                       col_itr++) {
                    type::Value val = src_old_tuple.GetValue(col_itr);
                    src_new_tuple.SetValue(col_itr, val, context->GetPool());
                  }

                  // Set the primary key fields
                  for (oid_t k = 0; k < key_attrs.size(); k++) {
                    auto src_col_index = key_attrs[k];
                    auto sink_col_index = cons->GetFKSinkColumnIds()[k];
                    src_new_tuple.SetValue(src_col_index,
                                           new_tuple->GetValue(sink_col_index),
                                           context->GetPool());
                  }
                }

                ItemPointer new_loc = src_table->InsertEmptyVersion();

                if (new_loc.IsNull()) {
                  if (src_is_owner == false) {
                    transaction_manager.YieldOwnership(
                        current_txn, src_tile_group_header, ptr->offset);
                  }
                  return false;
                }

                transaction_manager.PerformDelete(current_txn, *ptr, new_loc);

                // For delete cascade, just stop here
                if (is_update == false) {
                  break;
                }

                ItemPointer *index_entry_ptr = nullptr;
                peloton::ItemPointer location = src_table->InsertTuple(
                    &src_new_tuple, current_txn, &index_entry_ptr, false);

                if (location.block == INVALID_OID) {
                  return false;
                }

                transaction_manager.PerformInsert(current_txn, location,
                                                  index_entry_ptr);

                break;
              }
            }
          }
        }

        break;
      }
    }
  }

  return true;
}

// PA - looks like the FIXME has been done. We check to see if the key
// is visible
/**
 * @brief Check if all the foreign key constraints on this table
 * is satisfied by checking whether the key exist in the referred table
 *
 * FIXME: this still does not guarantee correctness under concurrent transaction
 *   because it only check if the key exists the referred table's index
 *   -- however this key might be a uncommitted key that is not visible to
 *   and it might be deleted if that txn abort.
 *   We should modify this function and add logic to check
 *   if the result of the ScanKey is visible.
 *
 * @returns True on success, false if any foreign key constraints fail
 */
bool DataTable::CheckForeignKeyConstraints(
    const AbstractTuple *tuple, concurrency::TransactionContext *transaction) {
  for (auto foreign_key : schema->GetForeignKeyConstraints()) {
    oid_t sink_table_id = foreign_key->GetFKSinkTableOid();
    storage::DataTable *ref_table = nullptr;
    try {
      ref_table = (storage::DataTable *)storage::StorageManager::GetInstance()
                      ->GetTableWithOid(database_oid, sink_table_id);
    } catch (CatalogException &e) {
      LOG_ERROR("Can't find table %d! Return false", sink_table_id);
      return false;
    }
    int ref_table_index_count = ref_table->GetIndexCount();

    for (int index_itr = ref_table_index_count - 1; index_itr >= 0;
         --index_itr) {
      auto index = ref_table->GetIndex(index_itr);
      if (index == nullptr) continue;

      // The foreign key constraints only refer to the primary key
      if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
        std::vector<oid_t> key_attrs = foreign_key->GetFKSinkColumnIds();
        std::unique_ptr<catalog::Schema> foreign_key_schema(
            catalog::Schema::CopySchema(ref_table->schema, key_attrs));
        std::unique_ptr<storage::Tuple> key(
            new storage::Tuple(foreign_key_schema.get(), true));
        key->SetFromTuple(tuple, foreign_key->GetColumnIds(), index->GetPool());

        LOG_TRACE("check key: %s", key->GetInfo().c_str());
        std::vector<ItemPointer *> location_ptrs;
        index->ScanKey(key.get(), location_ptrs);

        // if this key doesn't exist in the referred column
        if (location_ptrs.size() == 0) {
          LOG_DEBUG("The key: %s does not exist in table %s\n",
                    key->GetInfo().c_str(), ref_table->GetName().c_str());
          return false;
        }

        // Check the visibility of the result
        auto tile_group = ref_table->GetTileGroupById(location_ptrs[0]->block);
        auto tile_group_header = tile_group->GetHeader();

        auto &transaction_manager =
            concurrency::TransactionManagerFactory::GetInstance();
        auto visibility = transaction_manager.IsVisible(
            transaction, tile_group_header, location_ptrs[0]->offset,
            VisibilityIdType::READ_ID);

        if (visibility != VisibilityType::OK) {
          LOG_DEBUG(
              "The key: %s is not yet visible in table %s, visibility "
              "type: %s.\n",
              key->GetInfo().c_str(), ref_table->GetName().c_str(),
              VisibilityTypeToString(visibility).c_str());
          return false;
        }

        break;
      }
    }
  }

  return true;
}

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void DataTable::IncreaseTupleCount(const size_t &amount) {
  number_of_tuples_ += amount;
  dirty_ = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseTupleCount(const size_t &amount) {
  number_of_tuples_ -= amount;
  dirty_ = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetTupleCount(const size_t &num_tuples) {
  number_of_tuples_ = num_tuples;
  dirty_ = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
size_t DataTable::GetTupleCount() const { return number_of_tuples_; }

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool DataTable::IsDirty() const { return dirty_; }

/**
 * @brief Reset dirty flag
 */
void DataTable::ResetDirty() { dirty_ = false; }

//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

TileGroup *DataTable::GetTileGroupWithLayout(
    std::shared_ptr<const Layout> layout) {
  oid_t tile_group_id =
      storage::StorageManager::GetInstance()->GetNextTileGroupId();
  return (AbstractTable::GetTileGroupWithLayout(database_oid, tile_group_id,
                                                layout, tuples_per_tilegroup_));
}

oid_t DataTable::AddDefaultIndirectionArray(
    const size_t &active_indirection_array_id) {
  auto &manager = catalog::Manager::GetInstance();
  oid_t indirection_array_id = manager.GetNextIndirectionArrayId();

  std::shared_ptr<IndirectionArray> indirection_array(
      new IndirectionArray(indirection_array_id));
  manager.AddIndirectionArray(indirection_array_id, indirection_array);

  COMPILER_MEMORY_FENCE;

  active_indirection_arrays_[active_indirection_array_id] = indirection_array;

  return indirection_array_id;
}

oid_t DataTable::AddDefaultTileGroup() {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  return AddDefaultTileGroup(active_tile_group_id);
}

oid_t DataTable::AddDefaultTileGroup(const size_t &active_tile_group_id) {
  oid_t tile_group_id = INVALID_OID;

  // Create a tile group with that partitioning
  std::shared_ptr<TileGroup> tile_group(
      GetTileGroupWithLayout(default_layout_));
  PELOTON_ASSERT(tile_group.get());

  tile_group_id = tile_group->GetTileGroupId();

  LOG_TRACE("Added a tile group ");
  tile_groups_.Append(tile_group_id);

  //added by zhangqian on 2021-5 tile_group_latest,tile_group_id_latest
  // add tile group metadata in locator
//  storage::StorageManager::GetInstance()->AddTileGroup(tile_group_id,
//                                                       tile_group);
    storage::StorageManager::GetInstance()->AddTileGroup(database_oid,table_oid,
                                                         tile_group_count_,
                                                         tile_group_id,
                                                         tile_group,
                                                         this->tile_group_pre);



  COMPILER_MEMORY_FENCE;

  active_tile_groups_[active_tile_group_id] = tile_group;
  tile_group_array_.push_back(tile_group.get());
  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);

  this->tile_group_pre = tile_group;
  this->tile_group_id_latest = tile_group_id;
//  LOG_DEBUG("Recording latest tile group : %u , %p", tile_group_id, &tile_group_latest);

  return tile_group_id;
}

void DataTable::AddTileGroupWithOidForRecovery(const oid_t &tile_group_id) {
  PELOTON_ASSERT(tile_group_id);

  std::vector<catalog::Schema> schemas;
  schemas.push_back(*schema);
  std::shared_ptr<const Layout> layout = nullptr;

  // The TileGroup for recovery is always added in ROW layout,
  // This was a part of the previous design. If you are planning
  // to change this, make sure the layout is added to the catalog
  if (default_layout_->IsRowStore()) {
    layout = default_layout_;
  } else {
    layout = std::shared_ptr<const Layout>(
        new const Layout(schema->GetColumnCount()));
  }

  std::shared_ptr<TileGroup> tile_group(TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, schemas, layout,
      tuples_per_tilegroup_));

  auto tile_groups_exists = tile_groups_.Contains(tile_group_id);

  if (tile_groups_exists == false) {
    tile_groups_.Append(tile_group_id);

    LOG_TRACE("Added a tile group ");

    // add tile group metadata in locator
    storage::StorageManager::GetInstance()->AddTileGroup(tile_group_id,
                                                         tile_group);

    // we must guarantee that the compiler always add tile group before adding
    // tile_group_count_.
    COMPILER_MEMORY_FENCE;

    tile_group_count_++;

    LOG_TRACE("Recording tile group : %u ", tile_group_id);
  }
}

// NOTE: This function is only used in test cases.
void DataTable::AddTileGroup(const std::shared_ptr<TileGroup> &tile_group) {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;

  active_tile_groups_[active_tile_group_id] = tile_group;

  oid_t tile_group_id = tile_group->GetTileGroupId();

  tile_groups_.Append(tile_group_id);

  //added by zhangqian on tile_group_latest,tile_group_id_latest
  // add tile group in catalog
  storage::StorageManager::GetInstance()->AddTileGroup(tile_group_id,
                                                       tile_group);

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);

//  tile_group_latest = tile_group;
//  tile_group_id_latest = tile_group_id;
//  LOG_DEBUG("Recording latest tile group : %u , %p", tile_group_id, &tile_group_latest);

}

size_t DataTable::GetTileGroupCount() const { return tile_group_count_; }

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroup(
    const std::size_t &tile_group_offset) const {
  PELOTON_ASSERT(tile_group_offset < GetTileGroupCount());

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  return GetTileGroupById(tile_group_id);
}
//oid_t DataTable::GetTileGroupIdByOffset(const std::size_t &tile_group_offset) const{
//  auto tile_group_id =
//      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);
//  return tile_group_id;
//}

std::shared_ptr<storage::TileGroup> DataTable::GetTileGroupById(
    const oid_t &tile_group_id) const {
  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetTileGroup(tile_group_id);
}
//added by zhangqian on 2021-05 GetTileGroupByList GetTileGroupByTree
std::vector<storage::TileGroup *> DataTable::GetTileGroupList(oid_t tile_group_count) const {
  std::shared_ptr<storage::TileGroup> tile_group_pre_ = this->tile_group_pre;
  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetTileGroupByList(tile_group_pre_,tile_group_count);
}
std::vector<storage::TileGroup *> DataTable::GetTileGroupsBTree(oid_t table_id, oid_t tile_group_count) const {
  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetTileGroupsByBTree(table_id,tile_group_count);
}
storage::TileGroup *DataTable::GetTileGroupBTree(oid_t table_id, oid_t tile_group_offset) const {
  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetTileGroupByBTree(table_id,tile_group_offset);
}
//std::vector<ItemPointer *> DataTable::GetTileGroupBwTree(oid_t table_id,
//                                                         type::Value low_,
//                                                         type::Value high_,
//                                                         bool isPoint) const {
//  auto storage_manager = storage::StorageManager::GetInstance();
////  return storage_manager->GetTileGroupByBwTree(table_id, low_,high_,isPoint);
//}
//std::vector<storage::TileGroup *> DataTable::GetTileGroupBwTreeFull() const {
//
//  return this->tile_group_array_;
//}

void DataTable::DropTileGroups() {
  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      // drop tile group in catalog
      storage_manager->DropTileGroup(tile_group_id);
    }
  }

  // Clear array
  tile_groups_.Clear();

  tile_group_count_ = 0;
}

//===--------------------------------------------------------------------===//
// INDEX
//===--------------------------------------------------------------------===//

void DataTable::AddIndex(std::shared_ptr<index::Index> index) {
  // Add index
  indexes_.Append(index);

  // Add index column info
  auto index_columns_ = index->GetMetadata()->GetKeyAttrs();
  std::set<oid_t> index_columns_set(index_columns_.begin(),
                                    index_columns_.end());

  indexes_columns_.push_back(index_columns_set);
}

std::shared_ptr<index::Index> DataTable::GetIndexWithOid(
    const oid_t &index_oid) {
  std::shared_ptr<index::Index> ret_index;
  auto index_count = indexes_.GetSize();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    ret_index = indexes_.Find(index_itr);
    if (ret_index != nullptr && ret_index->GetOid() == index_oid) {
      break;
    }
  }
  if (ret_index == nullptr) {
    throw CatalogException("No index with oid = " + std::to_string(index_oid) +
                           " is found");
  }
  return ret_index;
}

void DataTable::DropIndexWithOid(const oid_t &index_oid) {
  oid_t index_offset = 0;
  std::shared_ptr<index::Index> index;
  auto index_count = indexes_.GetSize();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    index = indexes_.Find(index_itr);
    if (index != nullptr && index->GetOid() == index_oid) {
      break;
    }
  }

  PELOTON_ASSERT(index_offset < indexes_.GetSize());

  // Drop the index
  indexes_.Update(index_offset, nullptr);

  // Drop index column info
  indexes_columns_[index_offset].clear();
}

void DataTable::DropIndexes() {
  // TODO: iterate over all indexes, and actually drop them

  indexes_.Clear();

  indexes_columns_.clear();
}

// This is a dangerous function, use GetIndexWithOid() instead. Note
// that the returned index could be a nullptr once we can drop index
// with oid (due to a limitation of LockFreeArray).
std::shared_ptr<index::Index> DataTable::GetIndex(const oid_t &index_offset) {
  PELOTON_ASSERT(index_offset < indexes_.GetSize());
  auto ret_index = indexes_.Find(index_offset);

  return ret_index;
}

//
std::set<oid_t> DataTable::GetIndexAttrs(const oid_t &index_offset) const {
  PELOTON_ASSERT(index_offset < GetIndexCount());

  auto index_attrs = indexes_columns_.at(index_offset);

  return index_attrs;
}

oid_t DataTable::GetIndexCount() const {
  size_t index_count = indexes_.GetSize();

  return index_count;
}

oid_t DataTable::GetValidIndexCount() const {
  std::shared_ptr<index::Index> index;
  auto index_count = indexes_.GetSize();
  oid_t valid_index_count = 0;

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    index = indexes_.Find(index_itr);
    if (index == nullptr) {
      continue;
    }

    valid_index_count++;
  }

  return valid_index_count;
}

// Get the schema for the new transformed tile group
std::vector<catalog::Schema> TransformTileGroupSchema(
    storage::TileGroup *tile_group, const Layout &layout) {
  std::vector<catalog::Schema> new_schema;
  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;
  auto tile_group_layout = tile_group->GetLayout();

  // First, get info from the original tile group's schema
  std::map<oid_t, std::map<oid_t, catalog::Column>> schemas;

  uint32_t column_count = layout.GetColumnCount();
  for (oid_t col_id = 0; col_id < column_count; col_id++) {
    // Get TileGroup layout's tile and offset for col_id.
    tile_group_layout.LocateTileAndColumn(col_id, orig_tile_offset,
                                          orig_tile_column_offset);
    // Get new layout's tile and offset for col_id.
    layout.LocateTileAndColumn(col_id, new_tile_offset, new_tile_column_offset);

    // Get the column info from original tile
    auto tile = tile_group->GetTile(orig_tile_offset);
    PELOTON_ASSERT(tile != nullptr);
    auto orig_schema = tile->GetSchema();
    auto column_info = orig_schema->GetColumn(orig_tile_column_offset);
    schemas[new_tile_offset][new_tile_column_offset] = column_info;
  }

  // Then, build the new schema
  for (auto schemas_tile_entry : schemas) {
    std::vector<catalog::Column> columns;
    for (auto schemas_column_entry : schemas_tile_entry.second)
      columns.push_back(schemas_column_entry.second);

    catalog::Schema tile_schema(columns);
    new_schema.push_back(tile_schema);
  }

  return new_schema;
}

// Set the transformed tile group column-at-a-time
void SetTransformedTileGroup(storage::TileGroup *orig_tile_group,
                             storage::TileGroup *new_tile_group) {
  auto new_layout = new_tile_group->GetLayout();
  auto orig_layout = orig_tile_group->GetLayout();

  // Check that both tile groups have the same schema
  // Currently done by checking that the number of columns are equal
  // TODO Pooja: Handle schema equality for multiple schema versions.
  UNUSED_ATTRIBUTE auto new_column_count = new_layout.GetColumnCount();
  UNUSED_ATTRIBUTE auto orig_column_count = orig_layout.GetColumnCount();
  PELOTON_ASSERT(new_column_count == orig_column_count);

  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;

  auto column_count = new_column_count;
  auto tuple_count = orig_tile_group->GetAllocatedTupleCount();
  // Go over each column copying onto the new tile group
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    // Locate the original base tile and tile column offset
    orig_layout.LocateTileAndColumn(column_itr, orig_tile_offset,
                                    orig_tile_column_offset);

    new_layout.LocateTileAndColumn(column_itr, new_tile_offset,
                                   new_tile_column_offset);

    auto orig_tile = orig_tile_group->GetTile(orig_tile_offset);
    auto new_tile = new_tile_group->GetTile(new_tile_offset);

    // Copy the column over to the new tile group
    for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
      type::Value val =
          (orig_tile->GetValue(tuple_itr, orig_tile_column_offset));
      new_tile->SetValue(val, tuple_itr, new_tile_column_offset);
    }
  }

  // Finally, copy over the tile header
  auto header = orig_tile_group->GetHeader();
  auto new_header = new_tile_group->GetHeader();
  *new_header = *header;
}

storage::TileGroup *DataTable::TransformTileGroup(
    const oid_t &tile_group_offset, const double &theta) {
  // First, check if the tile group is in this table
  if (tile_group_offset >= tile_groups_.GetSize()) {
    LOG_ERROR("Tile group offset not found in table : %u ", tile_group_offset);
    return nullptr;
  }

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  // Get orig tile group from catalog
  auto storage_tilegroup = storage::StorageManager::GetInstance();
  auto tile_group = storage_tilegroup->GetTileGroup(tile_group_id);
  auto diff = tile_group->GetLayout().GetLayoutDifference(*default_layout_);

  // Check threshold for transformation
  if (diff < theta) {
    return nullptr;
  }

  LOG_TRACE("Transforming tile group : %u", tile_group_offset);

  // Get the schema for the new transformed tile group
  auto new_schema =
      TransformTileGroupSchema(tile_group.get(), *default_layout_);

  // Allocate space for the transformed tile group
  std::shared_ptr<storage::TileGroup> new_tile_group(
      TileGroupFactory::GetTileGroup(
          tile_group->GetDatabaseId(), tile_group->GetTableId(),
          tile_group->GetTileGroupId(), tile_group->GetAbstractTable(),
          new_schema, default_layout_, tile_group->GetAllocatedTupleCount()));

  // Set the transformed tile group column-at-a-time
  SetTransformedTileGroup(tile_group.get(), new_tile_group.get());

  // Set the location of the new tile group
  // and clean up the orig tile group
  storage_tilegroup->AddTileGroup(tile_group_id, new_tile_group);

  return new_tile_group.get();
}

void DataTable::RecordLayoutSample(const tuning::Sample &sample) {
  // Add layout sample
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    layout_samples_.push_back(sample);
  }
}

std::vector<tuning::Sample> DataTable::GetLayoutSamples() {
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    return layout_samples_;
  }
}

void DataTable::ClearLayoutSamples() {
  // Clear layout samples list
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    layout_samples_.clear();
  }
}

void DataTable::RecordIndexSample(const tuning::Sample &sample) {
  // Add index sample
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    index_samples_.push_back(sample);
  }
}

std::vector<tuning::Sample> DataTable::GetIndexSamples() {
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    return index_samples_;
  }
}

void DataTable::ClearIndexSamples() {
  // Clear index samples list
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    index_samples_.clear();
  }
}

void DataTable::AddTrigger(trigger::Trigger new_trigger) {
  trigger_list_->AddTrigger(new_trigger);
}

int DataTable::GetTriggerNumber() {
  return trigger_list_->GetTriggerListSize();
}

trigger::Trigger *DataTable::GetTriggerByIndex(int n) {
  if (trigger_list_->GetTriggerListSize() <= n) return nullptr;
  return trigger_list_->Get(n);
}

trigger::TriggerList *DataTable::GetTriggerList() {
  if (trigger_list_->GetTriggerListSize() <= 0) return nullptr;
  return trigger_list_.get();
}

void DataTable::UpdateTriggerListFromCatalog(
    concurrency::TransactionContext *txn) {
  trigger_list_ = catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(database_oid)
      ->GetTriggerCatalog()
      ->GetTriggers(txn, table_oid);
}

hash_t DataTable::Hash() const {
  auto oid = GetOid();
  hash_t hash = HashUtil::Hash(&oid);
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(GetName().c_str(), GetName().length()));
  auto db_oid = GetOid();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&db_oid));
  return hash;
}

bool DataTable::Equals(const storage::DataTable &other) const {
  return (*this == other);
}

bool DataTable::operator==(const DataTable &rhs) const {
  if (GetName() != rhs.GetName()) return false;
  if (GetDatabaseOid() != rhs.GetDatabaseOid()) return false;
  if (GetOid() != rhs.GetOid()) return false;
  return true;
}

bool DataTable::SetCurrentLayoutOid(oid_t new_layout_oid) {
  oid_t old_oid = current_layout_oid_;
  while (old_oid <= new_layout_oid) {
    if (current_layout_oid_.compare_exchange_strong(old_oid, new_layout_oid)) {
      return true;
    }
    old_oid = current_layout_oid_;
  }
  return false;
}

}  // namespace storage
}  // namespace peloton
