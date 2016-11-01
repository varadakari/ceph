// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>


#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueRocksEnv.h"
#include "kv/KeyValueDB.h"
#include "os/kstore/kv.h"

#define NUM_OBJMAP_WRITERS 1
#define NUM_HEADER_WRITERS 1
#define NUM_TRIMMERS 1
#define NUM_KV_THREADS 1

namespace po = boost::program_options;
using namespace std;

string get_temp_bdev(uint64_t size)
{
  static int n = 0;
  string fn = "ceph_test_bluefs.tmp.block." + stringify(getpid())
    + "." + stringify(++n);
  int fd = ::open(fn.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0644);
  assert(fd >= 0);
  int r = ::ftruncate(fd, size);
  assert(r >= 0);
  ::close(fd);
  return fn;
}

char* gen_buffer(uint64_t size)
{
    char *buffer = new char[size];
    boost::random::random_device rand;
    rand.generate(buffer, buffer + size);
    return buffer;
}


void rm_temp_bdev(string f)
{
  ::unlink(f.c_str());
}

KeyValueDB* createdb(BlueFS *fs)
{
  rocksdb::Env *env =  new BlueRocksEnv(fs);
  KeyValueDB *db;
  char dbpath[PATH_MAX];
  strcpy(dbpath, "db");
  env->CreateDir(dbpath);
  env->CreateDir(string(dbpath) + ".wal");
  env->CreateDir(string(dbpath) + ".slow");
  
  db = KeyValueDB::create(g_ceph_context,
                          "rocksdb",
                          dbpath,  
                          static_cast<void*>(env));
  if (!db) {
    std::cerr << __func__ << " error creating db" << std::endl;
    // delete env manually here since we can't depend on db to do this
    // under this case
    delete env; 
    env = NULL;
    return NULL;
  }
  //string options;
  stringstream err;
#if 0
  //options = g_conf->bluestore_rocksdb_options;
  string options = ""
			  "create_if_missing=true;"
			  "max_write_buffer_number=16;"
			  "max_background_compactions=4;"
			  "stats_dump_period_sec = 5;"
			  "min_write_buffer_number_to_merge = 3;"
			  "level0_file_num_compaction_trigger = 4;"
			  "compression = kNoCompression;"
			  "recycle_log_file_num=16;";
#endif
  string options = ""
			  "max_write_buffer_number=16;"
			  "min_write_buffer_number_to_merge=2;"
			  "recycle_log_file_num=16;"
			  "compaction_threads=8;"
			  "flusher_threads=4;"
			  "max_background_compactions=8;"
			  "max_background_flushes=4;"
			  "max_bytes_for_level_base=5368709120;"
			  "write_buffer_size=83886080;"
			  "level0_file_num_compaction_trigger=2;"
			  "level0_slowdown_writes_trigger=400;"
			  "level0_stop_writes_trigger=800;"
			  "compression = kNoCompression;"
			  "stats_dump_period_sec=10;";
  db->init(options);
  int r = db->create_and_open(err);
  if (r) {
    std::cerr << __func__ << " error creating db" << std::endl;
    return NULL;
  }
  return db;
}

#define BLUEFS_START 1048576

#if 0
TEST(RocksBlueFS, CreateDB) {
  string fn = "/dev/sdd";
  BlueFS fs;
  KeyValueDB *db;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
  fs.add_block_extent(BlueFS::BDEV_DB, BLUEFS_START, fs.get_block_device_size(BlueFS::BDEV_DB) - BLUEFS_START);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  
  db = createdb(&fs);
  if (!db) {
    std::cerr << __func__ << " error creating db" << std::endl;
    assert(0);
  }
  fs.umount();
}
#endif

boost::random::mt19937 gen;
uint64_t maxobjects = 256000;
uint64_t maxpgs = 32;
uint64_t maxoffset = 1023;
int runtime;
int num_writers;
std::atomic_ullong inc_wal_seq(0);
std::atomic_ullong dec_wal_seq(0);
std::atomic_ullong  num_deletes(0);
std::map<uint64_t, uint64_t> pglog_map; // pg to pglog
std::map<uint64_t, uint64_t> pg_trim; // pg to previous trim 
string block_name_prefix("rbd_data.10046b8b4567");
string objmap_name_prefix("rbd_object_map.10046b8b4567");
string header_name_prefix("rbd_header.10046b8b4567");
bufferlist pglog_buf, pginfo_buf, onode_buf, bitmap_buf, statfs_buf;
bufferlist wal_buf, objmap_onode_buf;
bufferlist header_onode_buf; //3k
bool stop = false;
bool kv_sync_queue = false;
bool enable_trim = false;
deque<KeyValueDB::Transaction> kv_queue, kv_committing;
std::mutex kv_lock;
std::mutex pglog_lock;
std::string device;

uint64_t get_nid()
{
  //depending on size of the image, select an nid in the range
  boost::random::uniform_int_distribution<> dist(maxpgs+100, maxobjects+maxpgs+100);
  return dist(gen);
}

uint64_t get_objmap_nid()
{
  boost::random::uniform_int_distribution<> dist(maxobjects+maxpgs+101, maxobjects+maxpgs+166);
  return dist(gen);
}

uint64_t get_cid()
{
  boost::random::uniform_int_distribution<> dist(0, maxpgs);
  return dist(gen);
}

uint64_t get_pglog_version(int cid)
{
   std::unique_lock<std::mutex> l(pglog_lock);
   uint64_t val = pglog_map[cid];
   pglog_map[cid] += 1;
   return val;
}
void dump_pglog_map()
{
  cout << "  ******** PGLOG MAP *****  " << std::endl;
  for (std::map<uint64_t, uint64_t>::iterator it=pglog_map.begin(); it!=pglog_map.end(); ++it)
      std::cout << it->first << " => " << it->second << '\n';

  cout << "  ******** PGTRIM MAP *****  " << std::endl;
  for (std::map<uint64_t, uint64_t>::iterator it=pg_trim.begin(); it!=pg_trim.end(); ++it)
      std::cout << it->first << " => " << it->second << '\n';
}

uint64_t get_rand_offset()
{
  boost::random::uniform_int_distribution<> dist(0, maxoffset);
  return (dist(gen) * 4096);
}

#define  PGLOG_SIZE 185
#define  PGINFO_SIZE 850
#define  DATA_ONODE 430
#define  BITMAP_BUF 16
#define  STAT_BUF 40
#define  OBJMAP_ONODE 710
#define  WAL_BUF 12370
#define  HEADER_ONODE 3300

bufferlist gen_buffer_list(uint64_t size)
{
  bufferlist bl;
  char *buf = gen_buffer(size);
  bufferptr bp = buffer::claim_char(size, buf);
  bl.push_back(bp);
  return bl;
}
void generate_buffers()
{
  pglog_buf = gen_buffer_list(PGLOG_SIZE);
  pginfo_buf = gen_buffer_list(PGINFO_SIZE);
  onode_buf = gen_buffer_list(DATA_ONODE);
  bitmap_buf = gen_buffer_list(BITMAP_BUF);
  statfs_buf = gen_buffer_list(STAT_BUF);

  objmap_onode_buf = gen_buffer_list(OBJMAP_ONODE);
  wal_buf = gen_buffer_list(WAL_BUF);

  header_onode_buf = gen_buffer_list(HEADER_ONODE);
}

#if 0
void release_buffers()
{
  delete pglog_buf;
  delete pginfo_buf;
  delete onode_buf;
  delete bitmap_buf;
  delete statfs_buf;
  delete objmap_onode_buf;
  delete wal_buf;
  delete header_onode_buf;
}
#endif

static string pretty_binary_string(const string& in)
{
  char buf[10];
  string out; 
  out.reserve(in.length() * 3);
  enum { NONE, HEX, STRING } mode = NONE;
  unsigned from = 0, i;
  for (i=0; i < in.length(); ++i) {
    if ((in[i] < 32 || (unsigned char)in[i] > 126) ||
        (mode == HEX && in.length() - i >= 4 && 
         ((in[i] < 32 || (unsigned char)in[i] > 126) ||
          (in[i+1] < 32 || (unsigned char)in[i+1] > 126) ||
          (in[i+2] < 32 || (unsigned char)in[i+2] > 126) ||
          (in[i+3] < 32 || (unsigned char)in[i+3] > 126)))) {
      if (mode == STRING) {
        out.append(in.substr(from, i - from));
        out.push_back('\'');
      }    
      if (mode != HEX) {
        out.append("0x");
        mode = HEX; 
      }    
      if (in.length() - i >= 4) { 
        // print a whole u32 at once
        snprintf(buf, sizeof(buf), "%08x",
                 (uint32_t)(((unsigned char)in[i] << 24) |
                            ((unsigned char)in[i+1] << 16) |
                            ((unsigned char)in[i+2] << 8) | 
                            ((unsigned char)in[i+3] << 0)));
        i += 3;
      } else {
        snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
      }    
      out.append(buf);
    } else {
      if (mode != STRING) {
        out.push_back('\'');
        mode = STRING;
        from = i; 
      }    
    }    
  }
  if (mode == STRING) {
    out.append(in.substr(from, i - from));
    out.push_back('\'');
  }
  return out; 
}

static void _key_encode_shard(shard_id_t shard, string *key)
{
  key->push_back((char)((uint8_t)shard.id + (uint8_t)0x80));
}
static const char *_key_decode_shard(const char *key, shard_id_t *pshard)
{
  pshard->id = (uint8_t)*key - (uint8_t)0x80;
  return key + 1; 
}


static void append_escaped(const string &in, string *out)
{
  char hexbyte[8];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '#') {
      snprintf(hexbyte, sizeof(hexbyte), "#%02x", (unsigned)*i);
      out->append(hexbyte);
    } else if (*i >= '~') {
      snprintf(hexbyte, sizeof(hexbyte), "~%02x", (unsigned)*i);
      out->append(hexbyte);
    } else {
      out->push_back(*i);
    }    
  }
  out->push_back('!');
}

static int decode_escaped(const char *p, string *out)
{
  const char *orig_p = p; 
  while (*p && *p != '!') {
    if (*p == '#' || *p == '~') {
      unsigned hex; 
      int r = sscanf(++p, "%2x", &hex);
      if (r < 1) 
        return -EINVAL;
      out->push_back((char)hex);
      p += 2;
    } else {
      out->push_back(*p++);
    }    
  }
  return p - orig_p;
}

static int get_key_object(const string& key, ghobject_t *oid)
{
  int r;
  const char *p = key.c_str();

  if (key.length() < 1 + 8 + 4) 
    return -1;
  p = _key_decode_shard(p, &oid->shard_id);

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  oid->hobj.pool = pool - 0x8000000000000000ull;

  unsigned hash;
  p = _key_decode_u32(p, &hash);

  oid->hobj.set_bitwise_key_u32(hash);

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0) 
    return -2;
  p += r + 1; 

  string k;
  r = decode_escaped(p, &k); 
  if (r < 0) 
    return -3;
  p += r + 1; 
  if (*p == '=') {
    // no key
    ++p; 
    oid->hobj.oid.name = k; 
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p; 
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0) 
      return -5;
    p += r + 1; 
    oid->hobj.set_key(k);
  } else {
    // malformed
    return -6;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  p = _key_decode_u64(p, &oid->generation);
  if (*p) {
    // if we get something other than a null terminator here,
    // something goes wrong.
    return -7;
  }

  return 0;
}



static void get_object_key(const ghobject_t& oid, string *key)
{
  key->clear();

  _key_encode_shard(oid.shard_id, key);
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    append_escaped(oid.hobj.get_key(), key);
    // (ASCII chars < = and > sort in that order, yay)
    int r = oid.hobj.get_key().compare(oid.hobj.oid.name);
    if (r) {
      key->append(r > 0 ? ">" : "<");
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
    }    
  } else {
    // no key
    append_escaped(oid.hobj.oid.name, key);
    key->append("=");
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);

  // sanity check
  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      cerr << "  r " << r << std::endl;
      cerr << "key " << pretty_binary_string(*key) << std::endl;
      cerr << "oid " << oid << std::endl;
      cerr << "  t " << t << std::endl;
      assert(r == 0 && t == oid);
    }
  }
}

void make_offset_key(uint64_t offset, std::string *key)
{
    key->reserve(10);
   _key_encode_u64(offset, key);
}

uint64_t get_wal_seq()
{
 return inc_wal_seq++;
}

uint64_t get_dec_wal_seq()
{
 return dec_wal_seq++;
}

static void get_wal_key(uint64_t seq, string *out)
{
  _key_encode_u64(seq, out);
}


void generate_data_trx(KeyValueDB *db)
{
      uint64_t nid = get_nid();
      uint64_t cid = get_cid();
      uint64_t c_nid = cid + 100;
      uint64_t pglog_version = get_pglog_version(cid);
      string obj(block_name_prefix + stringify(nid) + stringify(pglog_version));
      ghobject_t hoid(hobject_t(sobject_t(obj, CEPH_NOSNAP)));
      string onode_key;
      get_object_key(hoid, &onode_key); 
      uint64_t offset = get_rand_offset();
      string off_key;
      make_offset_key(offset, &off_key);

      KeyValueDB::Transaction t = db->get_transaction();
      t->set("M", stringify(c_nid)+"."+stringify(cid)+"."+stringify(pglog_version), pglog_buf);
      t->set("M", stringify(c_nid) + "_info" , pginfo_buf);
      t->set("O", onode_key, onode_buf);
      t->merge("b", off_key, bitmap_buf);
      t->merge("T", "bluestore_statfs", statfs_buf);
      //std::this_thread::sleep_for(std::chrono::milliseconds(4));
      if (kv_sync_queue) {
	std::unique_lock<std::mutex> l(kv_lock);
	kv_queue.push_back(t);
      } else {
	db->submit_transaction_sync(t);
      }
}



void generate_objmap_trx(KeyValueDB *db)
{
      uint64_t nid = get_objmap_nid();
      uint64_t cid = get_cid();
      uint64_t c_nid = cid + 100;
      uint64_t pglog_version = get_pglog_version(cid);
      string obj(objmap_name_prefix+ stringify(nid) + stringify(pglog_version));
      ghobject_t hoid(hobject_t(sobject_t(obj, CEPH_NOSNAP)));
      string onode_key;
      get_object_key(hoid, &onode_key); 
      uint64_t wseq = get_wal_seq();
      string seq_key;
      get_wal_key(wseq, &seq_key);

      KeyValueDB::Transaction t = db->get_transaction();
      t->set("M", stringify(c_nid)+"."+stringify(cid)+"."+stringify(pglog_version), pglog_buf);
      t->set("M", stringify(c_nid) + "_info" , pginfo_buf);
      t->set("O", onode_key, objmap_onode_buf);
      t->set("L", seq_key, wal_buf);
      //std::this_thread::sleep_for(std::chrono::milliseconds(4));
      if (kv_sync_queue) {
	std::unique_lock<std::mutex> l(kv_lock);
	kv_queue.push_back(t);
      } else {
        db->submit_transaction(t);
      }
}

void remove_wal_key_tx(KeyValueDB *db)
{
    uint64_t wseq = get_dec_wal_seq();
    while (wseq == inc_wal_seq) {
      sleep(1);
    }
    string seq_key;
    //std::this_thread::sleep_for(std::chrono::milliseconds(4));
    get_wal_key(wseq, &seq_key);
    KeyValueDB::Transaction dt = db->get_transaction();
    dt->rm_single_key("L", seq_key);
    num_deletes++;
    if (kv_sync_queue) {
      std::unique_lock<std::mutex> l(kv_lock);
      kv_queue.push_back(dt);
    } else {
      db->submit_transaction_sync(dt);
    }

}

void generate_header_trx(KeyValueDB *db)
{
      uint64_t nid =  maxobjects + maxpgs + 66;
      uint64_t cid =  maxpgs - 25;
      uint64_t c_nid = cid + 100;
      uint64_t pglog_version = get_pglog_version(cid);
      string obj(header_name_prefix+ stringify(nid) + stringify(pglog_version));
      ghobject_t hoid(hobject_t(sobject_t(obj, CEPH_NOSNAP)));
      string onode_key;
      get_object_key(hoid, &onode_key); 

      KeyValueDB::Transaction t = db->get_transaction();
      t->set("M", stringify(c_nid)+"."+stringify(cid)+"."+stringify(pglog_version), pglog_buf);
      t->set("M", stringify(c_nid) + "_info" , pginfo_buf);
      t->set("O", onode_key, header_onode_buf);
      //std::this_thread::sleep_for(std::chrono::milliseconds(4));
      if (kv_sync_queue) {
        std::unique_lock<std::mutex> l(kv_lock);
	kv_queue.push_back(t);
      } else {
	db->submit_transaction(t);
	KeyValueDB::Transaction dt = db->get_transaction();
	db->submit_transaction_sync(dt);
      }
}

void generate_trim_trx(uint64_t cid, KeyValueDB *db)
{
	//trim atleast 100
	//remember the previous trim 
	//issue the trim
	uint64_t prev = pg_trim[cid];
	uint64_t c_nid = cid + 100;
        KeyValueDB::Transaction t = db->get_transaction();
	for(int i=0; i< 100; i++) {
	  t->rmkey("M", stringify(c_nid)+"."+stringify(cid)+"."+stringify(prev));
	  prev++;
	}
        //std::this_thread::sleep_for(std::chrono::milliseconds(4));
	if (kv_sync_queue) {
          std::unique_lock<std::mutex> l(kv_lock);
	  kv_queue.push_back(t);
	} else {
	  db->submit_transaction_sync(t);
	}
	pg_trim[cid] += 100;
	num_deletes += 100;
}

struct stats {
  uint64_t num_ops;
  uint64_t num_bytes;
  stats():num_ops(0),num_bytes(0){};
};
typedef struct stats stats_t;
std::map<uint64_t, stats_t> stats_map;

void trim_pglog(KeyValueDB *db)
{
  uint64_t num_trims = 0;
  while(!stop) {
    for (std::map<uint64_t, uint64_t>::iterator it=pglog_map.begin(); it!=pglog_map.end(); ++it) {
      if((it->second - pg_trim[it->first]) > 1000) {
	generate_trim_trx(it->first, db);
	num_trims++;
      }
    }
  }
 size_t sz = std::hash<std::thread::id>()(std::this_thread::get_id());
 struct stats a;
 a.num_ops = num_trims;
 std::unique_lock<std::mutex> l(pglog_lock);
 stats_map[sz] = a;
 cout <<" Number of trims from " << sz << " : " << num_trims << std::endl;
}

void do_join(std::thread& t)
{
    t.join();
}

void join_all(std::vector<std::thread>& v)
{
    std::for_each(v.begin(),v.end(),do_join);
}

void write_data(KeyValueDB *db)
{
    uint64_t num_writes = 0;
    while(!stop) {
      generate_data_trx(db);
      num_writes++;
    }
    size_t sz = std::hash<std::thread::id>()(std::this_thread::get_id());
    struct stats a;
    a.num_ops = num_writes;
    a.num_bytes = num_writes * (PGLOG_SIZE + PGINFO_SIZE + DATA_ONODE + BITMAP_BUF + STAT_BUF);
    std::unique_lock<std::mutex> l(pglog_lock);
    stats_map[sz] = a;
    cout <<" Number of writes from " << sz << " : " << num_writes << std::endl;
}


void write_objmap(KeyValueDB *db)
{
    uint64_t num_writes = 0;
    while(!stop) {
      generate_objmap_trx(db);
      num_writes++;
    }
    size_t sz = std::hash<std::thread::id>()(std::this_thread::get_id());
    struct stats a;
    a.num_ops = num_writes;
    a.num_bytes = num_writes * (PGLOG_SIZE + PGINFO_SIZE + OBJMAP_ONODE + WAL_BUF);
    std::unique_lock<std::mutex> l(pglog_lock);
    stats_map[sz] = a;
    cout <<" Number of obj map writes from " << sz << " : " << num_writes << std::endl;
}

void trim_objmap(KeyValueDB *db)
{
    uint64_t num_wal_deletes = 0;
    while(!stop) {
      remove_wal_key_tx(db);
      num_wal_deletes++;
    }
    size_t sz = std::hash<std::thread::id>()(std::this_thread::get_id());
    struct stats a;
    a.num_ops = num_wal_deletes;
    a.num_bytes = num_wal_deletes * WAL_BUF;
    std::unique_lock<std::mutex> l(pglog_lock);
    stats_map[sz] = a;
    cout <<" Number of wal deletes from " << sz << " : " << num_wal_deletes << std::endl;
}

void write_header(KeyValueDB *db)
{
    uint64_t num_writes = 0;
    while(!stop) {
      generate_header_trx(db);
      num_writes++;
    }
    size_t sz = std::hash<std::thread::id>()(std::this_thread::get_id());
    struct stats a;
    a.num_ops = num_writes;
    a.num_bytes = num_writes * (PGLOG_SIZE + PGINFO_SIZE + HEADER_ONODE);
    std::unique_lock<std::mutex> l(pglog_lock);
    stats_map[sz] = a;
    cout <<" Number of header writes from " << sz << " : " << num_writes << std::endl;
}

void kv_sync_thread(KeyValueDB *db)
{
  while(true) {
    assert(kv_committing.empty());
    if (kv_queue.empty()) {
      if (stop)
	break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    else {
      kv_lock.lock();
      kv_committing.swap(kv_queue);
      kv_lock.unlock();
      for (auto txc : kv_committing) {
	int r = db->submit_transaction(txc);
        assert(r == 0);
      }
      KeyValueDB::Transaction dt = db->get_transaction();
      int r = db->submit_transaction_sync(dt);
      assert(r == 0);
      //cout <<" Committed " << kv_committing.size() << " Transactions." << std::endl;
      kv_committing.clear();
    }
  }
}


void dump_stats()
{
  uint64_t total_ops = 0, total_bytes = 0;
  for(auto &it: stats_map) {
      std::cout << it.first << " => " << it.second.num_ops << '\t' << it.second.num_bytes <<'\n';
      total_ops += it.second.num_ops;
      total_bytes += it.second.num_bytes;
  }
  cout<<" Total ops: " << total_ops << "\n Total bytes:  "<< total_bytes << std::endl;
  cout <<" Total deletes: " << num_deletes << std::endl;
  cout << " OPs per sec: " << total_ops/runtime << std::endl;
  cout << " Bytes per sec: " << total_bytes/runtime << std::endl;
}


TEST(RocksBlueFS, test_1) {
  g_ceph_context->_conf->set_val(
    "rocksdb_cache_size",
    "1073741824");
  g_ceph_context->_conf->apply_changes(NULL);
  string fn = device;
  BlueFS fs;
  KeyValueDB *db;
  uuid_d fsid;
  fs.add_block_device(BlueFS::BDEV_DB, fn);
  fs.add_block_extent(BlueFS::BDEV_DB, BLUEFS_START, fs.get_block_device_size(BlueFS::BDEV_DB) - BLUEFS_START);
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  db = createdb(&fs);
  if (!db) {
    std::cerr << __func__ << " error creating db" << std::endl;
    assert(0);
  }
  generate_buffers();
  {
    std::vector<std::thread> write_threads;
    for (int i=0; i<num_writers; i++) {
      write_threads.push_back(std::thread(write_data, db));
    }

#if 0
    std::vector<std::thread> objmap_threads;
    for (int i=0; i<NUM_OBJMAP_WRITERS; i++) {
      objmap_threads.push_back(std::thread(write_objmap, db));
    }

    std::vector<std::thread> trim_objmap_threads;
    for (int i=0; i<NUM_OBJMAP_WRITERS; i++) {
      trim_objmap_threads.push_back(std::thread(trim_objmap, db));
    }

    std::vector<std::thread> header_threads;
    for (int i=0; i<NUM_HEADER_WRITERS; i++) {
      header_threads.push_back(std::thread(write_header, db));
    }
#endif

    std::vector<std::thread> trim_threads;
    if (enable_trim) {
      for (int i=0; i<NUM_TRIMMERS; i++) {
	trim_threads.push_back(std::thread(trim_pglog, db));
      }
    }

    std::vector<std::thread> kv_threads;
    if (kv_sync_queue) {
      for (int i=0; i<NUM_KV_THREADS; i++) {
	kv_threads.push_back(std::thread(kv_sync_thread, db));
      }
    }

    sleep(runtime);
    stop = true;

    join_all(write_threads);
#if 0
    join_all(objmap_threads);
    join_all(trim_objmap_threads);
    join_all(header_threads);
#endif
    join_all(trim_threads);
    if (kv_sync_queue) {
      join_all(kv_threads);
    }
  }
  dump_pglog_map();
  dump_stats();
  db->DumpStats();
  //dump the stats
  sleep(10);
  delete db;
  db = NULL;
  fs.DumpPerfStats();
  fs.umount();
}

int main(int argc, char **argv) {
  try 
  {
    po::options_description desc{"Options"};
    desc.add_options()
      ("help,h", "Help screen")
      ("runtime", po::value<uint16_t>()->default_value(120), "runtime")
      ("kvsync", po::value<bool>()->default_value(false), "KV Sync")
      ("enable-trim", po::value<bool>()->default_value(false), "Enable trimming pg log entries")
      ("num-writers", po::value<int>()->default_value(16), "Num Writers")
      ("device", po::value<std::string>()->default_value("/dev/null"), "Device");

    po::variables_map vm; 
    po::store(parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help"))
      std::cout << desc << '\n';
    if (vm.count("kvsync")) {
      kv_sync_queue = vm["kvsync"].as<bool>();
      std::cout << "Kv sync: " << kv_sync_queue << '\n';
    }
    if (vm.count("enable-trim")) {
      enable_trim = vm["enable-trim"].as<bool>();
      std::cout << "Enable trim: " << enable_trim << '\n';
    }
    if (vm.count("runtime")) {
      runtime = vm["runtime"].as<uint16_t>();
      std::cout << "Runtime: " << runtime << '\n';
    }
    if (vm.count("device")) {
      device = vm["device"].as<std::string>();
      std::cout << "Device: " << device << '\n';
    }
    if (vm.count("num-writers")) {
      num_writers = vm["num-writers"].as<int>();
      std::cout << "Num writers: " << num_writers << '\n';
    }
  }
  catch (const po::error &ex)
  {
    std::cerr << ex.what() << '\n';
    return -1;
  }

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  vector<const char *> def_args;
  def_args.push_back("--debug-bluefs=0/0");
  def_args.push_back("--debug-bdev=0/0");

  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      0);
  common_init_finish(g_ceph_context);

  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
