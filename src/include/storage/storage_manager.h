//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager.h
//
// Identification: src/include/storage/storage_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <atomic>
#include "common/container/cuckoo_map.h"
#include "common/internal_types.h"
#include "storage/tile_group.h"
#include "googlebtree/btree_map.h"
#include "masstree/masstree_btree.h"
#include "storage/data_table.h"
#include "masstree/varstr.h"
#include "hopscotchhashing/hopscotch_map.h"
#include "tbb/concurrent_hash_map.h"
#include "boost/functional/hash.hpp"
#include "type/value_factory.h"
#include <include/index/index_factory.h>
#include <include/index/scan_optimizer.h>
#include <list>
#include <functional>
#include "libcuckoo/cuckoohash_map.hh"


namespace peloton {

namespace storage {
  class TileGroup;
  class IndirectionArray;
}

namespace index {
class Index;
}

namespace storage {

class Database;
class DataTable;

//added by zhangqian on 2021-05

//NeighborhoodSize
#define PARTITION_SIZE_HOP  62

#define PARTITION_SIZE_TBB  100

#define PARTITION_SIZE_TREE  1000

//struct GoogleBtreeKey;
//struct TbbMapKey;
//struct TbbMapHashComp{
//  static size_t hash(const storage::TbbMapKey& tb) {
//    size_t h = 0;
//    std::string hash_table_name = tb.hash_table_name;
//    //type::Value key_hash = tb.hash_key_;
////    char *val_binary = new char[sizeof(uint32_t)];
////    key_hash.SerializeTo(val_binary, true, nullptr);
////    uint32_t hk = *reinterpret_cast<const int32_t *>(val_binary);
////    boost::hash_combine(h, hash_table_name);
////    boost::hash_combine(h, hk);
//    h = std::hash<std::string>{}(hash_table_name);
//
//    return h;
//  }
//  static bool equal(const storage::TbbMapKey& t1, const storage::TbbMapKey& t2){
//    if((t1.table_name == t2.table_name) && (t1.partition_id == t2.partition_id))
//    {
//      return true;
//    }
//    return false;
//  }
//
////  TbbMapHashComp(const TbbMapHashComp &) {}
////  TbbMapHashComp() {};
//};

class HopCotchHash{
 public:
  inline size_t operator()(const storage::HopscotchMapKey& hpp)  const{
    size_t h = 0;
//    type::Value partition_id_ = hpp.partition_id;
//    char *val_binary = new char[sizeof(uint32_t)];
//    partition_id_.SerializeTo(val_binary, true, nullptr);
//    uint32_t hk = *reinterpret_cast<const int32_t *>(val_binary);
    boost::hash_combine(h, hpp.table_id);
    boost::hash_combine(h, hpp.partition_id);
//    h = std::hash<oid_t>{}(hpp.table_id);
    return h;
  }

  /*
 * Copy constructor
 */
  HopCotchHash(const HopCotchHash &) {}
  HopCotchHash() {};

};
class HopCotchComp {
 public:
  inline bool operator()(const storage::HopscotchMapKey &hpp1,
                         const storage::HopscotchMapKey &hpp2) const {
    if ((hpp1.table_id == hpp2.table_id) && (hpp1.partition_id == hpp2.partition_id)) {
      return true;
    }
    return false;
  }
};
class CuckooHash{
 public:
  inline size_t operator()(const storage::CuckooMapKey& ck)  const{
    size_t h = 0;

    boost::hash_combine(h, ck.table_id);
    boost::hash_combine(h, ck.partition_id);
    return h;
  }

  /*
 * Copy constructor
 */
  CuckooHash(const CuckooHash &) {}
  CuckooHash() {};

};
class CuckooComp{
 public:
  inline bool operator()(const storage::CuckooMapKey& ck1, const storage::CuckooMapKey& ck2)  const{
    if((ck1.table_id == ck2.table_id) && (ck1.partition_id == ck2.partition_id)){
      return true;
    }
    return false;
  };
  /*
* Copy constructor
*/
  CuckooComp(const  CuckooComp &) {}
  CuckooComp() {};
};

/*
 * class GoogleBtreeKeyComparator - Compares tuple keys for less than relation
 *
 * This function is needed in all kinds of indices based on partial ordering
 * of keys. This invokation of the class instance returns true if one key
 * is less than another
 */
//class GoogleBtreeKeyComparator {
//
// public:
//
//  /*
//   * operator()() - Function invocation
//   *
//   * This function compares two keys
//   */
//  inline bool operator()(const GoogleBtreeKey &lhs, const GoogleBtreeKey &rhs) const {
//    // We assume two keys have the same schema (executor should guarantee this)
////    oid_t lh_table_id = lhs.table_id;
////    oid_t lh_column_id = lhs.column_id;
////    oid_t lh_tile_group_id = lhs.tile_group_id;
////
////    oid_t rh_table_id = rhs.table_id;
////    oid_t rh_column_id = rhs.column_id;
////    oid_t rh_tile_group_id = rhs.tile_group_id;
//
////    if(lh_table_id != rh_table_id) {return lh_table_id < rh_table_id;}
////    if(lh_column_id != rh_column_id) {return lh_column_id < rh_column_id;}
////    if(lh_tile_group_id != rh_tile_group_id){return lh_tile_group_id < rh_tile_group_id;}
//
//    // If we get here then two keys are equal. Still return false
//    index::CompactIntsComparator<2>
//    return lhs.aky < rhs.aky;
//  }
//
//  /*
//   * Copy constructor
//   */
//  GoogleBtreeKeyComparator(const GoogleBtreeKeyComparator &) {}
//  GoogleBtreeKeyComparator() {};
//};

class StorageManager {
 public:
  // Global Singleton
  static StorageManager *GetInstance(void);
  void init(){
    // added by zhangqian on 2021-5
    // create tile groups tree
//    std::vector<catalog::Column> tuple_column_list{};
//    std::vector<catalog::Column> key_column_list{};
//    //table,tile group,tile
//    catalog::Column column0(type::TypeId::INTEGER,
//                            4,
//                            "table_id", true);
//    catalog::Column column1(type::TypeId::INTEGER,
//                            4,
//                            "partition_id", true);
//    catalog::Column column2(type::TypeId::INTEGER,
//                            4,
//                            "block_id", true);
//    tuple_column_list.push_back(column0);
//    tuple_column_list.push_back(column1);
//    tuple_column_list.push_back(column2);
//    key_column_list.push_back(column0);
//    key_column_list.push_back(column1);
//    auto index_key_schema_ = new catalog::Schema(key_column_list);
//    auto index_tuple_schema_  = new catalog::Schema(tuple_column_list);
//    std::vector<oid_t> key_attrs = {0,1};
//    index_key_schema_->SetIndexedColumns(key_attrs);
//    auto index_metadata = new index::IndexMetadata(
//        "tile_group_index", 888999,  // Index oid
//        INVALID_OID, INVALID_OID, IndexType::BWTREE, IndexConstraintType::DEFAULT,
//        index_tuple_schema_, index_key_schema_, key_attrs, true);
//
//    //initilize a bwtree
//    tile_group_tree_ = index::IndexFactory::GetIndex(index_metadata);
//    LOG_DEBUG("storage manager tile group tree init();");

    //initilize a mass tree
    column_mass_tree_ = new ConcurrentMasstree();
  }

  // Deconstruct the catalog database when destroying the catalog.
  ~StorageManager();

  //===--------------------------------------------------------------------===//
  // DEPRECATED FUNCTIONs
  //===--------------------------------------------------------------------===//
  /*
  * We're working right now to remove metadata from storage level and eliminate
  * multiple copies, so those functions below will be DEPRECATED soon.
   */

  // Find a database using vector offset
  storage::Database *GetDatabaseWithOffset(oid_t database_offset) const;

  //===--------------------------------------------------------------------===//
  // GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
  //===--------------------------------------------------------------------===//

  /* Find a database using its oid from storage layer,
   * throw exception if not exists
   * */
  storage::Database *GetDatabaseWithOid(oid_t db_oid) const;

  /* Find a table using its oid from storage layer,
   * throw exception if not exists
   * */
  storage::DataTable *GetTableWithOid(oid_t database_oid,
                                      oid_t table_oid) const;

  /* Find a index using its oid from storage layer,
   * throw exception if not exists
   * */
  index::Index *GetIndexWithOid(oid_t database_oid, oid_t table_oid,
                                oid_t index_oid) const;

  //===--------------------------------------------------------------------===//
  // HELPERS
  //===--------------------------------------------------------------------===//
  // Returns true if the catalog contains the given database with the id
  bool HasDatabase(oid_t db_oid) const;
  oid_t GetDatabaseCount() { return databases_.size(); }

  //===--------------------------------------------------------------------===//
  // FUNCTIONS USED BY CATALOG
  //===--------------------------------------------------------------------===//

  void AddDatabaseToStorageManager(storage::Database *db) {
    databases_.push_back(db);
//    init();
  }

  bool RemoveDatabaseFromStorageManager(oid_t database_oid);

  void DestroyDatabases();

  //===--------------------------------------------------------------------===//
  // TILE GROUP ALLOCATION
  //===--------------------------------------------------------------------===//

  oid_t GetNextTileId() { return ++tile_oid_; }

  oid_t GetNextTileGroupId() { return ++tile_group_oid_; }

  oid_t GetCurrentTileGroupId() { return tile_group_oid_; }

  void SetNextTileGroupId(oid_t next_oid) { tile_group_oid_ = next_oid; }

  void AddTileGroup(oid_t database_id, oid_t table_id,
                    const oid_t tile_group_offset, const oid_t tile_group_id,
                    std::shared_ptr<storage::TileGroup> location,
                    std::shared_ptr<storage::TileGroup> tile_group_latest);
  void AddTileGroup(const oid_t oid,
                    std::shared_ptr<storage::TileGroup> location);

  void DropTileGroup(const oid_t oid);

  std::shared_ptr<storage::TileGroup> GetTileGroup(const oid_t oid);

  void ClearTileGroup(void);

  //tile group list, tile group Bwtree
  std::vector<storage::TileGroup *> GetTileGroupByList(std::shared_ptr<storage::TileGroup> tile_group_pre,
                                          oid_t current_);
  std::vector<storage::TileGroup *> GetTileGroupsByBTree(oid_t table_id, oid_t tile_group_count);
  storage::TileGroup *GetTileGroupByBTree(oid_t table_id, oid_t tile_group_offset);
//  std::vector<ItemPointer *> GetTileGroupByBwTree(oid_t table_id,
//                                                  type::Value low_,
//                                                  type::Value high_,
//                                                  bool isPoint);
  //google b-tree
  bool AddToGoogleBtree(index::CompactIntsKey<2> key,
                        storage::Tile *val);
  std::vector<std::vector<std::string>> GetGoogleTreeKValues(oid_t table_id,oid_t column_id,
                                                    oid_t tile_group_st,
                                                    oid_t tile_group_ed);
  std::vector<std::string> GetGoogleTreeKV(const oid_t table_id,
                                    const oid_t col_id,
                                    const oid_t ile_group_offset);
  //mass b+tree
  bool AddToMassBtree(concurrency::TransactionContext &tr,
                      const varstr &key,
                      storage::Tile *val);
  std::vector<std::vector<std::string>> GetMassBtreeKValues(concurrency::TransactionContext *tx,
                                                    const oid_t table_id,
                                                    const oid_t column_id,
                                                    const oid_t tile_group_st,
                                                    const oid_t tile_group_ed);
  std::vector<std::string> GetMassBtreeTuple(const oid_t table_id,
                                   const oid_t col_id,
                                   const oid_t tile_group_offset);
  //tile bwtree
//  bool AddToBwBtree(storage::Tuple *tuple_key, ItemPointer *itemptr);
    //hopscotch map
  bool AddToHopscotchMap(storage::HopscotchMapKey key_, storage::TileGroup *tile);
  storage::TileGroup *GetHopscotchKValue(storage::HopscotchMapKey &key_);
  //tbb concurrent map
//  bool AddToTbbConcurrentMap(storage::TbbMapKey key_, storage::TileGroup *tile);
//  storage::TileGroup *GetTbbConcurrentKValue(storage::TbbMapKey &key_);

  bool AddToCuckooMap(storage::CuckooMapKey key_, storage::TileGroup *tile);
  storage::TileGroup *GetCuckooKValue(storage::CuckooMapKey &key_);

 private:
  StorageManager();

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;

  //===--------------------------------------------------------------------===//
  // Data member for tile allocation
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> tile_oid_ = ATOMIC_VAR_INIT(START_OID);

  //===--------------------------------------------------------------------===//
  // Data members for tile group allocation
  //===--------------------------------------------------------------------===//
  std::atomic<oid_t> tile_group_oid_ = ATOMIC_VAR_INIT(START_OID);

  CuckooMap<oid_t, std::shared_ptr<storage::TileGroup>> tile_group_locator_;
  static std::shared_ptr<storage::TileGroup> empty_tile_group_;

  // added by zhangqian on 2021-5
  //0-1 table Tile Groups are organized in list
  std::list<storage::TileGroup *> tile_group_list_;
  //0-2 table Tile Groups are organized in B+tree
//  static index::Index *tile_group_tree_;
  GoogleBtree::btree_map<index::CompactIntsKey<2>, std::shared_ptr<storage::TileGroup>, index::CompactIntsComparator<2>> tile_group_tree_;

  //1-0 table columns are organized in google Btree
  GoogleBtree::btree_map<index::CompactIntsKey<2>, storage::Tile *, index::CompactIntsComparator<2>> column_google_tree_;
  typedef GoogleBtree::btree_map<index::CompactIntsKey<2>, storage::Tile *, index::CompactIntsComparator<2>>::iterator column_google_tree_iterator;
  //1-1 table columns are organized in mass Btree
  ConcurrentMasstree *column_mass_tree_;
//  static index::Index *tile_tree_;

  //2-0 table tuples are organized in HopscotchMap
  // default buckets number = DEFAULT_INIT_BUCKETS_SIZE = 1,
  // neighborhoods of each bucket = 62, overflow=list, growth policy= 2(power)
  // hash function=std::hash<key>(key%bucket size), each bucket stores the key/value and the bit-map
  tsl::hopscotch_map<storage::HopscotchMapKey, storage::TileGroup *, storage::HopCotchHash, storage::HopCotchComp> tuples_hopscotch_map_;
  //2-1 tbb concurrent hash map,
  // buckets number = no default, node size = key/value || mutex || next pointer
  // lookup return the key/value, not the hash value
//  tbb::concurrent_hash_map<storage::TbbMapKey, storage::Tile *, storage::TbbMapHashComp> tuples_tbb_map_;
//  typedef tbb::concurrent_hash_map<storage::TbbMapKey, storage::Tile *, storage::TbbMapHashComp>::const_accessor tuples_tbb_map_accessor;
  CuckooMap<storage::CuckooMapKey , storage::TileGroup *, storage::CuckooHash, storage::CuckooComp> tuples_cuckoo_map_;
};
}  // namespace
}
