#pragma once

#include "stdlib.h"
#include "stdio.h"
#include "stdlib.h"
#include "type/type.h"
#include "config-debug.h"
#include "cstddef"
#include "cstdio"
#include "cstdlib"
//#include "str_arena.h"
#include "string_slice.hh"
#include "masstree/string_slice.hh"

namespace peloton {

class varstr {
  friend std::ostream &operator<<(std::ostream &o, const varstr &k);

 public:
  inline varstr() : l(0), p(NULL) {}

  inline varstr(const uint8_t *p, uint32_t l) : l(l), p(p) {}
  inline varstr(const char *p, uint32_t l) : l(l), p((const uint8_t *)p) {}
  inline void copy_from(const uint8_t *s, uint32_t m) {
    copy_from((const char *)s, m);
  }
  inline void copy_from(const varstr *v) { copy_from(v->p, v->l); }

  inline void copy_from(const char *s, uint32_t ll) {
    if (ll) {
      memcpy((void *)p, s, ll);
    }
    l = ll;
  }

  inline bool operator==(const varstr &that) const {
    if (size() != that.size()) return false;
    return memcmp(data(), that.data(), size()) == 0;
  }

  inline bool operator!=(const varstr &that) const { return !operator==(that); }

  inline bool operator<(const varstr &that) const {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() < that.size());
  }

  inline bool operator>=(const varstr &that) const { return !operator<(that); }

  inline bool operator<=(const varstr &that) const {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() <= that.size());
  }

  inline bool operator>(const varstr &that) const { return !operator<=(that); }

  inline int compare(const varstr &that) const {
    return memcmp(data(), that.data(), std::min(size(), that.size()));
  }

  inline uint64_t slice() const {
    uint64_t ret = 0;
    uint8_t *rp = (uint8_t *)&ret;
    for (uint32_t i = 0; i < std::min(l, uint64_t(8)); i++) rp[i] = p[i];
    return static_cast<uint64_t>(ret);
  }

  //#ifdef MASSTREE
  inline uint64_t slice_at(int pos) const {
    return string_slice<uint64_t>::make_comparable((const char *)p + pos,
                                                   std::min(int(l - pos), 8));
  }
  //#endif

  inline varstr shift() const {
    PELOTON_ASSERT(l >= 8);
    return varstr(p + 8, l - 8);
  }

  inline varstr shift_many(uint32_t n) const {
    PELOTON_ASSERT(l >= 8 * n);
    return varstr(p + 8 * n, l - 8 * n);
  }

  inline uint32_t size() const { return l; }
  inline const uint8_t *data() const { return p; }
  inline uint8_t *data() { return (uint8_t *)(void *)p; }
  inline bool empty() const { return not size(); }

//#ifdef MASSTREE
  inline operator lcdf::Str() const { return lcdf::Str(p, l); }
//#endif

  inline void prefetch() {
    uint32_t i = 0;
    do {
      Eigen::internal::prefetch((const char *)(p + i));
      i += CACHE_LINE_SIZE;
    } while (i < l);
  }
  const std::string GetInfo() const {
    std::ostringstream os;
    os << "CompactIntsKey< varstr" <<   "> - " << l << " bytes"
       << std::endl;

    // This is the current offset we are on printing the key
    size_t offset = 0;
    while (offset < l) {
      constexpr int byte_per_line = 16;
      os << StringUtil::Format("0x%.8X    ", offset);

      for (int i = 0; i < byte_per_line; i++) {
        if (offset >= l) {
          break;
        }
        os << StringUtil::Format("%.2X ", p[offset]);
        // Add a delimiter on the 8th byte
        if (i == 7) {
          os << "   ";
        }
        offset++;
      }  // FOR
      os << std::endl;
    }  // WHILE

    return (os.str());
  }

  uint64_t l;
  //  fat_ptr ptr;
  const uint8_t *p;  // must be the last field
};
//varstr &str(str_arena &a, uint64_t size) { return *a.next(size); }

}  // namespace peloton
