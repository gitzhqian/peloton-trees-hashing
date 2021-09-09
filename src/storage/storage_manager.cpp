//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager.cpp
//
// Identification: src/storage/storage_manager.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/storage_manager.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/tile_group.h"
#include "type/value_factory.h"
#include "hopscotchhashing/hopscotch_map.h"
#include <iterator>
#include <list>

namespace peloton {
namespace storage {

std::shared_ptr<storage::TileGroup> StorageManager::empty_tile_group_;
//index::Index *StorageManager::tile_group_tree_;

StorageManager::StorageManager() = default;

StorageManager::~StorageManager() = default;

// Get instance of the global catalog storage manager
StorageManager *StorageManager::GetInstance() {
  static StorageManager global_catalog_storage_manager;
  return &global_catalog_storage_manager;
}

//===--------------------------------------------------------------------===//
// GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
//===--------------------------------------------------------------------===//

/* Find a database using its oid from storage layer,
 * throw exception if not exists
 * */
Database *StorageManager::GetDatabaseWithOid(oid_t database_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == database_oid) return database;
  throw CatalogException("Database with oid = " + std::to_string(database_oid) +
                         " is not found");
  return nullptr;
}

/* Find a table using its oid from storage layer,
 * throw exception if not exists
 * */
DataTable *StorageManager::GetTableWithOid(oid_t database_oid,
                                           oid_t table_oid) const {
  LOG_TRACE("Getting table with oid %d from database with oid %d", database_oid,
            table_oid);
  // Lookup DB from storage layer
  auto database =
      GetDatabaseWithOid(database_oid);  // Throw exception if not exists
  // Lookup table from storage layer
  return database->GetTableWithOid(table_oid);  // Throw exception if not exists
}

/* Find a index using its oid from storage layer,
 * throw exception if not exists
 * */
index::Index *StorageManager::GetIndexWithOid(oid_t database_oid,
                                              oid_t table_oid,
                                              oid_t index_oid) const {
  // Lookup table from storage layer
  auto table = GetTableWithOid(database_oid,
                               table_oid);  // Throw exception if not exists
  // Lookup index from storage layer
  return table->GetIndexWithOid(index_oid)
      .get();  // Throw exception if not exists
}

//===--------------------------------------------------------------------===//
// DEPRECATED
//===--------------------------------------------------------------------===//

// This is used as an iterator
Database *StorageManager::GetDatabaseWithOffset(oid_t database_offset) const {
  PELOTON_ASSERT(database_offset < databases_.size());
  auto database = databases_.at(database_offset);
  return database;
}

//===--------------------------------------------------------------------===//
// HELPERS
//===--------------------------------------------------------------------===//

// Only used for testing
bool StorageManager::HasDatabase(oid_t db_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == db_oid) return (true);
  return (false);
}

// Invoked when catalog is destroyed
void StorageManager::DestroyDatabases() {
  LOG_TRACE("Deleting databases");
  for (auto database : databases_) delete database;
  LOG_TRACE("Finish deleting database");
}

bool StorageManager::RemoveDatabaseFromStorageManager(oid_t database_oid) {
  for (auto it = databases_.begin(); it != databases_.end(); ++it) {
    if ((*it)->GetOid() == database_oid) {
      delete (*it);
      databases_.erase(it);
      return true;
    }
  }
  return false;
}

//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//
void StorageManager::AddTileGroup(
    const oid_t oid, std::shared_ptr<storage::TileGroup> location) {
  // add/update the catalog reference to the tile group
  tile_group_locator_.Upsert(oid, location);
}

//added by zhangqian on 2021-5
//add tile group in tile_group_list_ and tile_group_tree_
void StorageManager::AddTileGroup(oid_t database_id, oid_t table_id,
                                  const oid_t tile_group_offset, const oid_t tile_group_id,
                                  std::shared_ptr<storage::TileGroup> location,
                                  std::shared_ptr<storage::TileGroup> location_prev) {
  // add/update the catalog reference to the tile group
  tile_group_locator_.Upsert(tile_group_id, location);
//  LOG_DEBUG("tile group cuckoomap size: %zu ", tile_group_locator_.GetSize());

  // add table_id,partition_id,tile_group_id to the tile_group_tree
  // now we assume only one partition, tile_group = tile
  //key schema:table_id,partition_id(tile_group_offset)
//  std::unique_ptr<storage::Tuple> index_key(new storage::Tuple(tile_group_tree_->GetKeySchema(), true));
////  auto value_1 = type::ValueFactory::GetIntegerValue(table_id).Copy();
////  auto value_2 = type::ValueFactory::GetIntegerValue(tile_group_id).Copy();
//  auto value_0 = type::ValueFactory::GetIntegerValue(table_id).Copy();
//  auto value_1 = type::ValueFactory::GetIntegerValue(tile_group_offset).Copy();
////  index_key->SetValue(0, value_1, nullptr);
//  index_key->SetValue(0, value_0, nullptr);
//  index_key->SetValue(1, value_1, nullptr);
////  LOG_DEBUG("insert tree key%s", index_key->GetInfo().c_str());
//
//  //item=block_location
//  ItemPointer *index_entry_ptr_=new ItemPointer(tile_group_id,0);
//  index_entry_ptr_->SetLocation(location.get());
//
//  tile_group_tree_->InsertEntry(index_key.get(), index_entry_ptr_);

  // add tile group to the google tree tile group
 if(GetDatabaseWithOid(database_id)->GetDBName()=="default_database"){
   index::CompactIntsKey<2> key_g_;
   key_g_.AddInteger(table_id,0);
   key_g_.AddInteger(tile_group_offset,sizeof(table_id));
   tile_group_tree_.insert(std::make_pair(key_g_, location));
   // add tile_group_id to the table tile_group_list_
   if(location_prev != nullptr){
     auto tile_group_pre = std::find(tile_group_list_.begin(),
                                     tile_group_list_.end(), location_prev.get());
     if (tile_group_pre != tile_group_list_.end()) {
       tile_group_list_.insert(tile_group_pre, location.get());
     } else {
       tile_group_list_.push_back(location_prev.get());
       tile_group_list_.push_back(location.get());
     }
   }else{
     tile_group_list_.push_back(location.get());
   }
 }

//  LOG_DEBUG("tile group list size: %zu ", tile_group_list_.size());
}

void StorageManager::DropTileGroup(const oid_t oid) {
  // drop the catalog reference to the tile group
  tile_group_locator_.Erase(oid);
}

std::shared_ptr<storage::TileGroup> StorageManager::GetTileGroup(
    const oid_t oid) {
  std::shared_ptr<storage::TileGroup> location;
  if (tile_group_locator_.Find(oid, location)) {
    return location;
  }
  return empty_tile_group_;
}

// GetTileGroupList GetTileGroupTree
std::vector<storage::TileGroup *> StorageManager::GetTileGroupByList(
                          std::shared_ptr<storage::TileGroup> tile_group_pre,
                          oid_t tile_group_count) {
//  tile_group_list_.reverse();
//  std::list<storage::TileGroup *>::const_iterator tile_group_pre_itr;
  auto start_itr = tile_group_list_.begin();
  auto end_itr = tile_group_list_.end();
//  LOG_DEBUG("current access, %u",tile_group_count);

  std::vector<storage::TileGroup *> rt;
  for(auto it = start_itr; it !=end_itr; ++it){
    if((*it) == tile_group_pre.get()){
      for(oid_t i=0; i<tile_group_count; i++){
        storage::TileGroup *tg = *it;
        rt.push_back(tg);
      //  __builtin_prefetch(*it++,0,3);
      }
      break;
    }
  }

  return rt;
}
std::vector<storage::TileGroup *> StorageManager::GetTileGroupsByBTree(oid_t table_id, oid_t tile_group_count) {
  std::vector<storage::TileGroup *> val;
//  index::CompactIntsKey<2> key_g_;
//  key_g_.AddInteger(table_id,0);
//  key_g_.AddInteger(0,sizeof(table_id));
  index::CompactIntsKey<2> key_g_h;
  key_g_h.AddInteger(table_id,0);
  key_g_h.AddInteger(tile_group_count,sizeof(table_id));
//  auto begin_ = tile_group_tree_.lower_bound(key_g_);
//  auto end_ = tile_group_tree_.upper_bound(key_g_h);
//  for(auto itr_ = begin_; itr_!=end_; ++itr_){
//    val.push_back(itr_->second.get());
//  }
  tile_group_tree_.find(key_g_h)->second.get();


  return val;
}
storage::TileGroup *StorageManager::GetTileGroupByBTree(oid_t table_id, oid_t tile_group_offset) {
  storage::TileGroup *val;

  index::CompactIntsKey<2> key_g_h;
  key_g_h.AddInteger(table_id,0);
  key_g_h.AddInteger(tile_group_offset,sizeof(table_id));

  val = tile_group_tree_.find(key_g_h)->second.get();

  return val;
}
//std::vector<ItemPointer *> StorageManager::GetTileGroupByBwTree(oid_t table_id,
//                                                                type::Value low_,
//                                                                type::Value high_,
//                                                                bool isPoint) {
//  type::Value vl0 = type::ValueFactory::GetIntegerValue(table_id).Copy();
//  type::Value vl1_l = low_.Divide(type::ValueFactory::GetIntegerValue(TEST_TUPLES_PER_TILEGROUP)).Copy();
//  type::Value vl1_h = high_.Divide(type::ValueFactory::GetIntegerValue(TEST_TUPLES_PER_TILEGROUP)).Copy();
//  std::vector<ItemPointer *> result;
//  if(isPoint){
//    index::ConjunctionScanPredicate *predicate =
//        new index::ConjunctionScanPredicate(
//            tile_group_tree_, {vl1_l}, {1},
//            { ExpressionType::COMPARE_EQUAL});
//    tile_group_tree_->Scan({}, {}, {},
//                           ScanDirectionType::FORWARD, result, predicate);
//  }else{
//    index::ConjunctionScanPredicate *predicate =
//        new index::ConjunctionScanPredicate(
//            tile_group_tree_, {vl1_l, vl1_h}, {1,1},
//            { ExpressionType::COMPARE_GREATERTHANOREQUALTO, ExpressionType::COMPARE_LESSTHANOREQUALTO});
//    tile_group_tree_->Scan({}, {}, {},
//                           ScanDirectionType::FORWARD, result, predicate);
//  }
//
//  return result;
//}
//Google-key:table_id,column_itr,tile_group_offset
bool StorageManager::AddToGoogleBtree(index::CompactIntsKey<2> key, storage::Tile *val) {
  //find the position, and insert
//  auto ret=
  column_google_tree_.insert(column_google_tree_.find(key),
      std::pair<index::CompactIntsKey<2>, storage::Tile *>(key, val));
//  int r_ = ret.position;
//  LOG_DEBUG("insert position , %u",r_);
  return true;
}
std::vector<std::vector<std::string>> StorageManager::GetGoogleTreeKValues(oid_t table_id,
                                                                  oid_t column_id,
                                                                  oid_t tile_group_st,
                                                                  oid_t tile_group_ed) {
  std::vector<std::vector<std::string>> val;
  index::CompactIntsKey<2> key_g_l;
  key_g_l.AddInteger(table_id,0);
  key_g_l.AddInteger(column_id,sizeof(table_id));
  key_g_l.AddInteger(tile_group_st,(sizeof(table_id)+sizeof(column_id)));
  index::CompactIntsKey<2> key_g_h;
  key_g_h.AddInteger(table_id,0);
  key_g_h.AddInteger(column_id,sizeof(table_id));
  key_g_h.AddInteger(tile_group_ed,(sizeof(table_id)+sizeof(column_id)));
  auto begin_ = column_google_tree_.lower_bound(key_g_l);
  auto end_ = column_google_tree_.upper_bound(key_g_h);
  for(auto itr_ = begin_; itr_!=end_; ++itr_){
    storage::Tile *vl = itr_->second;
    val.push_back(vl->GetBlock(vl->GetAllocatedTupleCount()));
  }

  return val;
}

std::vector<std::string> StorageManager::GetGoogleTreeKV(const oid_t table_id,
                                                  const oid_t col_id,
                                                  const oid_t tile_group_offset) {
  std::vector<std::string> val_;
  index::CompactIntsKey<2> key_g_;
  key_g_.AddInteger(table_id,0);
  key_g_.AddInteger(col_id,sizeof(table_id));
  key_g_.AddInteger(tile_group_offset,(sizeof(table_id)+sizeof(col_id)));
  column_google_tree_iterator itr_value = column_google_tree_.find(key_g_);

//  index::CompactIntsKey<2> key_g_pre;
//  key_g_.AddInteger(table_id,0);
//  key_g_.AddInteger(col_id,sizeof(table_id));
//  key_g_.AddInteger((tile_group_offset+1),(sizeof(table_id)+sizeof(col_id)));
//  column_google_tree_.find(key_g_pre);

  if (itr_value != column_google_tree_.end()) {
    storage::Tile *vl = itr_value->second;
    val_ = vl->GetBlock(vl->GetAllocatedTupleCount());
  }

  return val_;
}
//Mass-tree-key:table_id_,column_id_,tile_group_offset_
//compare varstr each bytes
bool StorageManager::AddToMassBtree(concurrency::TransactionContext &tr,
                                    const varstr &key,
                                    storage::Tile *val) {
  PELOTON_ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  ConcurrentMasstree::insert_info_t insert_info;
//  LOG_DEBUG("tile location,%p, tile group id,%u,",tile,tile->GetTileGroupId());
  bool inserted =
      column_mass_tree_->insert_if_absent(key, val, tr, &insert_info);
  if (!inserted) {
    return false;
  }

  return true;
}
std::vector<std::vector<std::string>> StorageManager::GetMassBtreeKValues(concurrency::TransactionContext *tr,
                                                                  const oid_t table_id,
                                                                  const oid_t column_id,
                                                                  const oid_t tile_group_st,
                                                                  const  oid_t tile_group_ed) {
  PELOTON_ASSERT(tr.GetEpochId() >= 0);
  std::vector<std::vector<std::string>> vals ;
  if(tile_group_st == tile_group_ed){
    index::CompactIntsKey<2> key_m_;
    key_m_.AddInteger(table_id,0);
    key_m_.AddInteger(column_id,sizeof(table_id));
    key_m_.AddInteger(tile_group_st,(sizeof(table_id)+sizeof(column_id)));
    varstr *var_ = new varstr(key_m_.GetRawData(),key_m_.key_size_byte);
    varstr &varstr_ = *var_;
    storage::Tile *val_ = column_mass_tree_->search(varstr_, 0, nullptr);
    vals.push_back(val_->GetBlock(val_->GetAllocatedTupleCount()));
  }else{
    index::CompactIntsKey<2> key_m_l;
    key_m_l.AddInteger(table_id,0);
    key_m_l.AddInteger(column_id,sizeof(table_id));
    key_m_l.AddInteger(tile_group_st,(sizeof(table_id)+sizeof(column_id)));

    index::CompactIntsKey<2> key_m_h;
    key_m_h.AddInteger(table_id,0);
    key_m_h.AddInteger(column_id,sizeof(table_id));
    key_m_h.AddInteger(tile_group_ed,(sizeof(table_id)+sizeof(column_id)));

    varstr *var_l = new varstr(key_m_l.GetRawData(),key_m_l.key_size_byte);
    varstr &varstr_l = *var_l;
    varstr *var_h = new varstr(key_m_h.GetRawData(),key_m_h.key_size_byte);


    auto iter = ConcurrentMasstree::ScanIterator<
        /*IsRerverse=*/false>::factory(column_mass_tree_,
                                       tr,
                                       varstr_l, var_h);
    bool more = iter.init_or_next</*IsNext=*/false>();
    while(more){
      storage::Tile *vl = iter.value();
      vals.push_back(vl->GetBlock(vl->GetAllocatedTupleCount()));
      more = iter.init_or_next</*IsNext=*/true>();
    }
  }

  return vals;
}
std::vector<std::string> StorageManager::GetMassBtreeTuple(const oid_t table_id,
                                                 const oid_t col_id,
                                                 const oid_t ile_group_offset) {
  index::CompactIntsKey<2> key_m_;
  key_m_.AddInteger(table_id,0);
  key_m_.AddInteger(col_id,sizeof(table_id));
  key_m_.AddInteger(ile_group_offset,(sizeof(table_id)+sizeof(col_id)));
  varstr *var_ = new varstr(key_m_.GetRawData(),key_m_.key_size_byte);
  varstr &varstr_ = *var_;
  storage::Tile *val_=nullptr;
  val_ = column_mass_tree_->search(varstr_, 0, nullptr);

//  LOG_DEBUG("tile location,%p, tile group id,%u,",tile_,tile_->GetTileGroupId());

  return val_->GetBlock(val_->GetAllocatedTupleCount());
}
bool StorageManager::AddToHopscotchMap(storage::HopscotchMapKey key_, storage::TileGroup *tile){
//  tuples_hopscotch_map_.insert(tuples_hopscotch_map_.find(key_), );
  tuples_hopscotch_map_.insert(std::pair<storage::HopscotchMapKey,storage::TileGroup *>(key_,tile));
  if(tuples_hopscotch_map_.find(key_) != tuples_hopscotch_map_.end()){
    return true;
  }

  return false;
}
storage::TileGroup *StorageManager::GetHopscotchKValue(storage::HopscotchMapKey &key_) {
//  TileGroup("get hopscotch tuple key: %s, partition id: %s", key_.tuple_key.ToString().c_str(),key_.partition_id.ToString().c_str());
  storage::TileGroup *tile_= nullptr;
  size_t hash_ = 0;
  boost::hash_combine(hash_, key_.table_id);
  boost::hash_combine(hash_, key_.partition_id);
  auto itr = tuples_hopscotch_map_.find(key_,hash_);

  if(itr != tuples_hopscotch_map_.end()){
    tile_ = itr->second;
  }

  return tile_;
}
bool StorageManager::AddToCuckooMap(storage::CuckooMapKey key_, storage::TileGroup *tile){
  tuples_cuckoo_map_.Upsert(key_,tile);
  bool ret = tuples_cuckoo_map_.Contains(key_);
  return ret;
}
storage::TileGroup *StorageManager::GetCuckooKValue(storage::CuckooMapKey &key_) {
//  TileGroup("get hopscotch tuple key: %s, partition id: %s", key_.tuple_key.ToString().c_str(),key_.partition_id.ToString().c_str());
  storage::TileGroup *tile_= nullptr;
  tuples_cuckoo_map_.Find(key_,tile_);
//  if(ret==true)

  return tile_;
}
//bool StorageManager::AddToTbbConcurrentMap(storage::TbbMapKey key_, storage::Tile *tile){
//  auto is_new = tuples_tbb_map_.insert(std::make_pair(key_,tile));
//
//  return is_new;
//}
//storage::Tile *StorageManager::GetTbbConcurrentKValue(storage::TbbMapKey &key_){
////  LOG_DEBUG("get tbb tuple key: %s, partition id: %s", key_.tuple_key_.ToString().c_str(),key_.hash_key_.ToString().c_str());
//  storage::Tile *tile_=nullptr;
////  size_t bucket_count = tuples_tbb_map_.bucket_count();
//  //find return the key equal(table_name,tuple_key)
//
//  tuples_tbb_map_accessor accessor;
//  auto is_found = tuples_tbb_map_.find(accessor,key_);
//  if(is_found == true){
//    tile_ = accessor->second;
//    accessor.release();
//  }
//  return tile_;
//}

// used for logging test
void StorageManager::ClearTileGroup() { tile_group_locator_.Clear(); }

}  // namespace storage
}  // namespace peloton
