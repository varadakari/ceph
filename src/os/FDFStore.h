// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_FDFSTORE_H
#define CEPH_FDFSTORE_H

//#include <ext/hash_map>
//using namespace __gnu_cxx;
#include <tr1/unordered_map>
#define hash_map std::tr1::unordered_map

#include "include/assert.h"
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "ObjectStore.h"

#include <boost/thread/tss.hpp>

#include "zs.h"

class FDFStore : public ObjectStore {
public:
  /*
   * the encoding of FDF keys are as follows:
   *
   * 1. using ghobject_to_string to encode 
   *    a ghobject_t instance to string fromat
   * 2. ceph key for FDF key(read/write) is encoded as:
   *    ceph-key + "\1\1"
   * 3. attribute for FDF key is encoded as:
   *    ceph-key + "\1\2" + attribute
   * 4. omap for FDF key is encoded as:
   *    ceph-key + "\1\3" + omap_key
   *
   * collection attr for FDF key is encoded slightly different:
   *    prefix + ceph-key.to_str() + "\1\4" + collection-attr
   *
   * when using FDF Range API to get key/values, there is a 
   * separate range for each type of keys.
   */
  static const string DATA_SEPARATOR;
  static const string ATTR_SEPARATOR;
  static const string OMAP_SEPARATOR;
  static const string COLL_ATTR_SEPARATOR;
  static const string END_SEPARATOR;

  struct Collection {
    ZS_cguid_t cguid;

    Collection(ZS_cguid_t _cguid = ZS_NULL_CGUID) : cguid(_cguid) {}
  };
  typedef std::tr1::shared_ptr<Collection> CollectionRef;

private:
  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    FDFStore *store;
    struct ZS_thread_state *thrd_state_;
    CollectionRef c;
    const ghobject_t& oid_;
    ZS_status_t rc_;
    bool valid_;
    ZS_range_meta_t rmeta_;
    struct ZS_cursor *cursor_;
    ZS_range_data_t values_;
    string key_;
    bufferlist value_;
  public:
    OmapIteratorImpl(struct ZS_thread_state *thrd_state, CollectionRef c, const ghobject_t& oid)
      : thrd_state_(thrd_state),
        c(c),
        oid_(oid),
        rc_(ZS_SUCCESS),
        valid_(true) {
      seek_to_first();
    }

    int seek_to_first() {
      int n_out;
    
      valid_ = true;
      string oid_key;
      //store->ghobject_to_string(oid_, oid_key);
      FDFStore::ghobject_to_string(oid_, oid_key);
      memset(&rmeta_, 0, sizeof(rmeta_));
    
      ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
                                  (ZS_RANGE_START_GT | ZS_RANGE_END_LT);
      rmeta_.flags = flags;
    
      string key_start = oid_key + OMAP_SEPARATOR;
      size_t encoded_len = key_start.length();

      /*  1 is the trailing '\0' */
      rmeta_.key_start = (char *)calloc(1, (encoded_len + 1));
      assert(rmeta_.key_start);
      memcpy(rmeta_.key_start, key_start.c_str(), key_start.length());
      rmeta_.keylen_start = encoded_len;
    
      string key_end = oid_key + COLL_ATTR_SEPARATOR;
      encoded_len = key_end.length();

      /* 1 is the trailing '\0' */
      rmeta_.key_end = (char *)calloc(1, (encoded_len + 1));
      assert(rmeta_.key_end);
      memcpy(rmeta_.key_end, key_end.c_str(), key_end.length());
      rmeta_.keylen_end = encoded_len;
      
      /* Initiate the range */
      rc_ = ZSGetRange(thrd_state_, c->cguid, 
                          ZS_RANGE_PRIMARY_INDEX, &cursor_, &rmeta_);
      if( rc_ != ZS_SUCCESS) {
        derr << "FDF cguid(" << c->cguid << ") "
                  << __func__ << " failed for FDFGetRange: "
                  << ZSStrError(rc_) << dendl;
        free(rmeta_.key_start);
        free(rmeta_.key_end);
        valid_ = false;
        return 0;
      }

      free(rmeta_.key_start);
      free(rmeta_.key_end);
    
      memset(&values_, 0, sizeof(values_));

      rc_ = ZSGetNextRange(thrd_state_, cursor_, 1, &n_out, &values_);
      if( rc_ == ZS_SUCCESS || rc_ == ZS_QUERY_DONE) {
        if (n_out == 0) {
          valid_ = false;
          goto range_out;
        }
        /* get the attribute name of the endcoded key */
        string key_attr(values_.key, values_.keylen);
        if(key_attr.substr(0, oid_key.length()) == oid_key) {
          key_ = key_attr.substr(oid_key.length() + 2);
          value_.clear();
          bufferptr bp = buffer::create(values_.datalen);
          memcpy(bp.c_str(), values_.data, values_.datalen);
          value_.push_back(bp);  
        }

        ZSFreeBuffer(values_.key);
        ZSFreeBuffer(values_.data);
      } else {
        derr << "FDF cguid(" << c->cguid << ") "
             << __func__ << " failed for FDFGetNextRange: "
             << ZSStrError(rc_) << dendl;
        valid_ = false;
      }
range_out:
      /* Finish the enumeration */
      rc_ = ZSGetRangeFinish(thrd_state_, cursor_);
      if ( rc_ != ZS_SUCCESS ) {
        derr << "FDF cguid(" << c->cguid << ") "
             << __func__ << " failed for FDFGetNextRangeFinish: "
             << ZSStrError(rc_) << dendl;
        valid_ = false;
      }
      return 0;
    }
    int upper_bound(const string &after) {
      int n_out;
    
      //valid_ = true;
      string oid_key;
      //kstore->ghobject_to_string(oid_, oid_key);
      FDFStore::ghobject_to_string(oid_, oid_key);

      memset(&rmeta_, 0, sizeof(rmeta_));
    
      ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
                                (ZS_RANGE_START_GT | ZS_RANGE_END_LT);
      rmeta_.flags = flags;
    
      string key_start = oid_key + OMAP_SEPARATOR + after;
      size_t encoded_len = key_start.length();

      /*  1 is the trailing '\0' */
      rmeta_.key_start = (char *)calloc(1, (encoded_len + 1));
      assert(rmeta_.key_start);
      memcpy(rmeta_.key_start, key_start.c_str(), key_start.length());
      rmeta_.keylen_start = encoded_len;
    
      string key_end = oid_key + COLL_ATTR_SEPARATOR;
      encoded_len = key_end.length();

      /* 1 is the trailing '\0' */
      rmeta_.key_end = (char *)calloc(1, (encoded_len + 1));
      assert(rmeta_.key_end);
      memcpy(rmeta_.key_end, key_end.c_str(), key_end.length());
      rmeta_.keylen_end = encoded_len;
      
      /* Initiate the range */
      rc_ = ZSGetRange(thrd_state_, c->cguid, 
                          ZS_RANGE_PRIMARY_INDEX, &cursor_, &rmeta_);
      if( rc_ != ZS_SUCCESS) {
        derr << "FDF cguid(" << c->cguid << ") "
             << __func__ << " failed for FDFGetRange: "
             << ZSStrError(rc_) << dendl;
        free(rmeta_.key_start);
        free(rmeta_.key_end);
        valid_ = false;
        return 0;
      }

      free(rmeta_.key_start);
      free(rmeta_.key_end);
    
      memset(&values_, 0, sizeof(values_));

      rc_ = ZSGetNextRange(thrd_state_, cursor_, 1, &n_out, &values_);
      if( rc_ == ZS_SUCCESS || rc_ == ZS_QUERY_DONE) {
        if (n_out == 0) {
          valid_ = false;
          derr << "FDF cguid(" << c->cguid << ") "
               << __func__ << " failed for FDFGetNextRange: " << ZSStrError(rc_) << dendl;
          goto range_out;
        }
        /* get the attribute name of the endcoded key */
        string key_attr(values_.key, values_.keylen);
        if(key_attr.substr(0, oid_key.length()) == oid_key) {
          key_ = key_attr.substr(oid_key.length() + 2);
          value_.clear();
          bufferptr bp = buffer::create(values_.datalen);
          memcpy(bp.c_str(), values_.data, values_.datalen);
          value_.push_back(bp);  
        }
        ZSFreeBuffer(values_.key);
        ZSFreeBuffer(values_.data);
      } else {
        derr << "FDF cguid(" << c->cguid << ") "
                  << __func__ << " failed for FDFGetNextRange: "
                  << ZSStrError(rc_) << dendl;
        valid_ = false;
      }
range_out:
      /* Finish the enumeration */
      rc_ = ZSGetRangeFinish(thrd_state_, cursor_);
      if ( rc_ != ZS_SUCCESS ) {
        derr << "FDF cguid(" << c->cguid << ") "
                  << __func__ << " failed for FDFGetNextRangeFinish: "
                  << ZSStrError(rc_) << dendl;
        valid_ = false;
      }
      return 0;
    }

    int lower_bound(const string &to) {
      int n_out;
    
      string oid_key;
      FDFStore::ghobject_to_string(oid_, oid_key);

      memset(&rmeta_, 0, sizeof(rmeta_));
    
      ZS_range_enums_t flags = static_cast<ZS_range_enums_t>
                                (ZS_RANGE_START_GE | ZS_RANGE_END_LT);
      rmeta_.flags = flags;
    
      string key_start = oid_key + OMAP_SEPARATOR + to;
      size_t encoded_len = key_start.length();

      /*  1 is the trailing '\0' */
      rmeta_.key_start = (char *)calloc(1, (encoded_len + 1));
      assert(rmeta_.key_start);
      memcpy(rmeta_.key_start, key_start.c_str(), key_start.length());
      rmeta_.keylen_start = encoded_len;
    
      string key_end = oid_key + COLL_ATTR_SEPARATOR;
      encoded_len = key_end.length();

      /* 1 is the trailing '\0' */
      rmeta_.key_end = (char *)calloc(1, (encoded_len + 1));
      assert(rmeta_.key_end);
      memcpy(rmeta_.key_end, key_end.c_str(), key_end.length());
      rmeta_.keylen_end = encoded_len;
      
      /* Initiate the range */
      rc_ = ZSGetRange(thrd_state_, c->cguid, 
                          ZS_RANGE_PRIMARY_INDEX, &cursor_, &rmeta_);
      if( rc_ != ZS_SUCCESS) {
        derr << "FDF cguid(" << c->cguid << ") "
                  << __func__ << " failed for FDFGetRange: "
                  << ZSStrError(rc_) << dendl;
        free(rmeta_.key_start);
        free(rmeta_.key_end);
        valid_ = false;
        return 0;
      }

      free(rmeta_.key_start);
      free(rmeta_.key_end);
    
      memset(&values_, 0, sizeof(values_));

      rc_ = ZSGetNextRange(thrd_state_, cursor_, 1, &n_out, &values_);
      if( rc_ == ZS_SUCCESS || rc_ == ZS_QUERY_DONE) {
        if (n_out == 0) {
          valid_ = false;
          goto range_out;
        }
        /* get the attribute name of the endcoded key */
        string key_attr(values_.key, values_.keylen);
        if(key_attr.substr(0, oid_key.length()) == oid_key) {
          key_ = key_attr.substr(oid_key.length() + 2);
          value_.clear();
          bufferptr bp = buffer::create(values_.datalen);
          memcpy(bp.c_str(), values_.data, values_.datalen);
          value_.push_back(bp);  
        }

        ZSFreeBuffer(values_.key);
        ZSFreeBuffer(values_.data);
      } else {
        derr << "FDF cguid(" << c->cguid << ") "
                  << __func__ << " failed for FDFGetNextRange: "
                  << ZSStrError(rc_) << dendl;
        valid_ = false;
        goto range_out;
      }
range_out:
      /* Finish the enumeration */
      rc_ = ZSGetRangeFinish(thrd_state_, cursor_);
      if ( rc_ != ZS_SUCCESS ) {
        derr << "FDF cguid(" << c->cguid << ") "
                  << __func__ << " failed for FDFGetNextRangeFinish: "
                  << ZSStrError(rc_) << dendl;
        valid_ = false;
      }
      return 0;
    }
    bool valid() {
      return valid_;
    }
    int next() {
      return upper_bound(key_);
    }
    string key() {
      return key_;
    }
    bufferlist value() {
      return value_;
    }
    int status() {
      return 0;
    }
  };

  bool init_done;
  struct ZS_state *fdf_state;
  boost::thread_specific_ptr<struct ZS_thread_state> fdf_thd_state;

  hash_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map

  CollectionRef get_collection(coll_t cid);

  Finisher finisher;

  static int ghobject_to_string(const ghobject_t& oid, string& key);
  static int string_to_ghobject( char *str, ghobject_t *oid);

  void _do_transaction(Transaction& t);

  void _write_into_bl(const bufferlist& src, unsigned offset, bufferlist *dst);

  int _touch(coll_t cid, const ghobject_t& oid);
  int _write(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl,
      bool replica = false);
  int _zero(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(coll_t cid, const ghobject_t& oid, uint64_t size);
  int _remove(coll_t cid, const ghobject_t& oid);
  int _setattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);
  int _rmattr(coll_t cid, const ghobject_t& oid, const char *name);
  int _rmattrs(coll_t cid, const ghobject_t& oid);
  int _clone(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid);
  int _clone_range(coll_t cid, const ghobject_t& oldoid,
		   const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _omap_clear(coll_t cid, const ghobject_t &oid);
  int _omap_setkeys(coll_t cid, const ghobject_t &oid,
		    const map<string, bufferlist> &aset);
  int _omap_rmkeys(coll_t cid, const ghobject_t &oid, const set<string> &keys);
  int _omap_rmkeyrange(coll_t cid, const ghobject_t &oid,
		       const string& first, const string& last);
  int _omap_setheader(coll_t cid, const ghobject_t &oid, const bufferlist &bl);
  int _collection_hint_expected_num_objs(coll_t cid, uint32_t pg_num,
	uint64_t num_objs) const { return 0; }
  int _create_collection(coll_t c);
  int _destroy_collection(coll_t c);
  int _collection_add(coll_t cid, coll_t ocid, const ghobject_t& oid);
  int _collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
			      coll_t cid, const ghobject_t& o);
  int _collection_setattr(coll_t cid, const char *name, const void *value,
			  size_t size);
  int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset);
  int _collection_rmattr(coll_t cid, const char *name);
  int _collection_rename(const coll_t &cid, const coll_t &ncid);
  int _split_collection(coll_t cid, uint32_t bits, uint32_t rem, coll_t dest);

  void fdf_write_to_mput_wrapper(ZS_cguid_t, char*, uint32_t, char*, uint64_t);
  int _mput();

  int _save();
  int _load();

  void dump(Formatter *f);
  void dump_all();

  ZS_status_t fdf_object_exists(ZS_thread_state*, ZS_cguid_t, char*, uint32_t, int*, int*, uint64_t*);
  ZS_status_t fdf_object_list(ZS_thread_state*, ZS_cguid_t, char*, uint32_t, uint64_t, int, char***, int*);
  ZS_status_t fdf_object_range(ZS_thread_state*, ZS_cguid_t, char*, uint32_t, uint64_t, uint64_t, char**, int, uint64_t*, uint64_t*);
  ZS_status_t fdf_object_join(ZS_thread_state*, ZS_cguid_t, char*, uint32_t, uint64_t, uint64_t, char**, char**, int);
  void fdf_object_free_list(char**, int);
  ZS_status_t fdf_object_delete_list(ZS_thread_state*, ZS_cguid_t, char*, uint32_t, char**, int);
  ZS_status_t fdf_object_split(ZS_thread_state*, ZS_cguid_t, char*, uint32_t, char*, uint64_t, uint64_t);
  
public:
  FDFStore(CephContext *cct, const string& path)
    : ObjectStore(path),
      init_done(false),
      coll_lock("FDFStore::coll_lock"),
      finisher(cct) { }
  ~FDFStore() {
    RWLock::RLocker l(coll_lock); // block any writer
    struct ZS_thread_state *thread_state = fdf_get_thd_state();
    if (thread_state != NULL) {
      hash_map<coll_t,CollectionRef>::iterator p;
      for (p = coll_map.begin(); p != coll_map.end(); ++p) {
        ZSFlushContainer(fdf_get_thd_state(), p->second->cguid);
        ZSCloseContainer(fdf_get_thd_state(), p->second->cguid);
      }
    }
    ZSShutdown(fdf_state);
  }

  int update_version_stamp() {
    return 0;
  }
  uint32_t get_target_version() {
    return 1;
  }

  int peek_journal_fsid(uuid_d *fsid);

  bool test_mount_in_use() {
    return false;
  }

  int mount();
  int umount();

  unsigned get_max_object_name_length() {
    return 4096;
  }

  unsigned get_max_attr_name_length() {
    return 1024;
  }

  bool need_journal() {
    return false;
  }

  int mkfs();
  int mkjournal() {
    return 0;
  }

  void set_allow_sharded_objects() {
  }
  bool get_allow_sharded_objects() {
    return true;
  }

  int statfs(struct statfs *buf);

  bool exists(coll_t cid, const ghobject_t& oid);
  int stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  int read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false);
  int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value);
  //int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset, bool user_only = false);
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(coll_t c);
  int collection_getattr(coll_t cid, const char *name,
			 void *value, size_t size);
  int collection_getattr(coll_t cid, const char *name, bufferlist& bl);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);
  bool collection_empty(coll_t c);
  int collection_list(coll_t cid, vector<ghobject_t>& o);
  int collection_list_partial(coll_t cid, ghobject_t start,
			      int min, int max, snapid_t snap, 
			      vector<ghobject_t> *ls, ghobject_t *next);
  int collection_list_range(coll_t cid, ghobject_t start, ghobject_t end,
			    snapid_t seq, vector<ghobject_t> *ls);

  struct ZS_thread_state *fdf_get_thd_state();
  CollectionRef fdf_get_container_for_coll(coll_t cid);

  int omap_get(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    coll_t cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    coll_t cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    coll_t cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    );

  void set_fsid(uuid_d u);
  uuid_d get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  int queue_transactions(
    Sequencer *osr, list<Transaction*>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL);
};




#endif
