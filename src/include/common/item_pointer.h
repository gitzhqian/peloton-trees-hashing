
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer.h
//
// Identification: src/include/common/item_pointer.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <include/storage/tile.h>

#include <cstdint>

#include "internal_types.h"

namespace peloton {
namespace storage{
class TileGroup;
}

// logical physical location
class ItemPointer {
 public:
  // block
  oid_t block;

  // 0-based offset within block
  oid_t offset;

  // added by zhangqian on 2021-05 block location
  storage::TileGroup *tile_group_location_;

  storage::Tile *tile_location_;

  ItemPointer() : block(INVALID_OID), offset(INVALID_OID),tile_group_location_(nullptr),tile_location_(nullptr) {}

  ItemPointer(oid_t block, oid_t offset) : block(block), offset(offset),tile_group_location_(nullptr),tile_location_(nullptr) {}

  bool IsNull() const {
    return (block == INVALID_OID && offset == INVALID_OID);
  }

  bool operator<(const ItemPointer &rhs) const {
    if (block != rhs.block) {
      return block < rhs.block;
    } else {
      return offset < rhs.offset;
    }
  }

  bool operator==(const ItemPointer &rhs) const {
    return (block == rhs.block && offset == rhs.offset);
  }

  void SetLocation(storage::TileGroup *tile_group_location){tile_group_location_ = tile_group_location;}
  void SetTileLocation(storage::Tile *tile_location){tile_location_ = tile_location;}

} __attribute__((__aligned__(8))) __attribute__((__packed__));

extern ItemPointer INVALID_ITEMPOINTER;

class ItemPointerComparator {
 public:
  bool operator()(ItemPointer *const &p1, ItemPointer *const &p2) const {
    return (p1->block == p2->block) && (p1->offset == p2->offset);
  }

  bool operator()(ItemPointer const &p1, ItemPointer const &p2) const {
    return (p1.block == p2.block) && (p1.offset == p2.offset);
  }

  ItemPointerComparator(const ItemPointerComparator &) {}
  ItemPointerComparator() {}
};

struct ItemPointerHasher {
  size_t operator()(const ItemPointer &item) const {
    // This constant is found in the CityHash code
    // [Source libcuckoo/default_hasher.hh]
    // std::hash returns the same number for unsigned int which causes
    // too many collisions in the Cuckoohash leading to too many collisions
    return (std::hash<oid_t>()(item.block)*0x9ddfea08eb382d69ULL) ^
      std::hash<oid_t>()(item.offset);
  }
};

class ItemPointerHashFunc {
 public:
  size_t operator()(ItemPointer *const &p) const {
    return (std::hash<oid_t>()(p->block)*0x9ddfea08eb382d69ULL) ^
    std::hash<oid_t>()(p->offset);
  }

  ItemPointerHashFunc(const ItemPointerHashFunc &) {}
  ItemPointerHashFunc() {}
};

bool AtomicUpdateItemPointer(ItemPointer *src_ptr, const ItemPointer &value);

}  // namespace peloton
