// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Western Digital Corporation
 *
 * Author: Allen Samuels <allen.samuels@sandisk.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <stdio.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "os/bluestore/bluestore_types.h"
#include "os/bluestore/BlueStore.h"
#include "include/enc_dec.h"


static size_t break_point = 0;

void dumpbuf(string s,bufferlist& b) {
  cout << s;
  for (size_t i = 0; i < b.length(); ++i) cout << " " << hex << (int)(b[i]) << dec;
  cout << "\n";
}

template<typename t> void enc_dec_buffer(t v) {

   bufferlist b;
   enc(b,v);
   //
   // Now decode :)
   //
   t d;

   break_point = (break_point + 1) % 7;

   bufferlist b1,b2,b3;
   bufferlist::iterator it = b.begin();

   if (break_point == 0 || break_point >= b.length()) {
     it = b.begin();
   } else {
     it.copy(break_point,b1);
     it.copy(b.length()-break_point,b2);
     b3.claim_append(b1);
     b3.claim_append(b2);
     it = b3.begin();
   }

   dec(it,d);
   EXPECT_EQ(v,d);
}


template<typename t> bool enc_dec_scalar(t v) {
   char buf[1000];

   for (size_t i = 0; i < sizeof(buf); ++i) buf[i] = (char) i; // poison the buffer :)

   size_t estimate = enc_dec(size_t(0), v);
   EXPECT_TRUE(estimate < sizeof(buf));
   
   char *end = enc_dec(buf,v);
   
   size_t actual = end - buf;
   EXPECT_TRUE(actual < sizeof(buf));

   for (size_t i = estimate; i < sizeof(buf); ++i) EXPECT_EQ((char)i,buf[i]); // verify not exceeded estimate

   //
   // Now decode :)
   //
   t dec;
   const char *done = enc_dec((const char *)buf,dec);
   size_t consumed = done - buf;
   EXPECT_EQ(consumed,actual);

   EXPECT_EQ(v,dec);

   return estimate == actual;
}


template<typename t> bool enc_dec_varint(t v) {

   char buf[1000];
   for (size_t i = 0; i < sizeof(buf); ++i) buf[i] = (char) i; // poison the buffer :)

   size_t estimate = enc_dec_varint(size_t(0), v);
   EXPECT_TRUE(estimate < sizeof(buf));
   
   char *end = enc_dec_varint(buf,v);
   
   size_t actual = end - buf;
   EXPECT_TRUE(actual < sizeof(buf));

   for (size_t i = estimate; i < sizeof(buf); ++i) EXPECT_EQ((char)i,buf[i]); // verify not exceeded estimate
   //
   // Now decode :)
   //
   t d;
   const char *done = enc_dec_varint((const char *)buf,d);
   size_t consumed = done - buf;
   EXPECT_EQ(consumed,actual);

   EXPECT_EQ(v,d);

   return estimate == actual; // this is how we detect whether is_bounded_size is triggered...
      
}

template<typename t> bool enc_dec_int(t v) {
   enc_dec_buffer(v);
   EXPECT_TRUE(enc_dec_scalar(v)); // always true
   return enc_dec_varint(v); // sometimes true
}

bool enc_dec_test(unsigned long long v) {
   bool always_equal = true;
   always_equal &= enc_dec_int((char) v);
   always_equal &= enc_dec_int((unsigned char) v);
   always_equal &= enc_dec_int((short) v);
   always_equal &= enc_dec_int((unsigned short) v);
   always_equal &= enc_dec_int((int)v);
   always_equal &= enc_dec_int((unsigned)v);
   if (v <= 0xFFFFFFFF) {
      always_equal &= enc_dec_int((size_t)v);
   }
   always_equal &= enc_dec_int((long long)v);
   always_equal &= enc_dec_int((unsigned long long) v);
   return always_equal;
}

const unsigned long long SIGN_BIT = 0x8000000000000000ull;

TEST(test_enc_dec, int)
{
   bool always_equal = true;
   always_equal &= enc_dec_test(0);
   always_equal &= enc_dec_test(SIGN_BIT);

   for (unsigned long long i = 1; i != 0; i <<= 1) {
      always_equal &= enc_dec_test(i);
      always_equal &= enc_dec_test(i | SIGN_BIT);
   }

   for (unsigned long long i = 1; i != ~0ull; i = (i << 1) | 1) {
      always_equal &= enc_dec_test(i);
      always_equal &= enc_dec_test(i | SIGN_BIT);
   }

   for (unsigned long long i = 0; i != ~0ull; i = (i >> 1) | SIGN_BIT) {
      always_equal &= enc_dec_test(i);
      always_equal &= enc_dec_test(i | SIGN_BIT);
   }

   EXPECT_FALSE(always_equal); // somewhere estimate != actual :)

}

TEST(test_enc_dec, string)
{
   EXPECT_TRUE(enc_dec_scalar(string()));
   EXPECT_TRUE(enc_dec_scalar(string("1")));
   EXPECT_TRUE(enc_dec_scalar(string("\0"))); //; tricky :)
   EXPECT_TRUE(enc_dec_scalar(string("123")));
   EXPECT_TRUE(enc_dec_scalar(string("123\0")));
}

 struct set_temp {
    set<int> s1;
    DECLARE_ENC_DEC_MEMBER_FUNCTION();
    bool operator==(const set_temp& s) const { return s.s1 == s1; }
 };

DECLARE_ENC_DEC_CLASS(set_temp)
DEFINE_ENC_DEC_MEMBER_FUNCTION(set_temp) {
   p = enc_dec(p,s1);
   return p;
}

TEST(test_enc_dec, map) 
{
   map<int,int> m1;
   m1[1] = 1;
   m1[2] = 2;
   EXPECT_TRUE(enc_dec_scalar(m1));

   map<string,string> s;
   s["a"] = "b";
   EXPECT_TRUE(enc_dec_scalar(s));   

   map<int,set_temp> m2;

   EXPECT_TRUE(enc_dec_scalar(m2));
}

TEST(test_enc_dec, set) 
{
   set<int> s1;
   s1.insert(1);
   EXPECT_TRUE(enc_dec_scalar(s1));

   set_temp t;
   t.s1.insert(3);
   t.s1.insert(-1);
   enc_dec_scalar(t);

   set<string> s;
   s.insert("b");
   EXPECT_TRUE(enc_dec_scalar(s));   

}


TEST(test_enc_dec, buff1) {
   int i = 42;
   bufferlist b;
   enc(b,i);

   //
   // decode it
   //

   bufferlist::iterator it = b.begin();

   int z;
   dec(it,z);
   EXPECT_EQ(z,i);

}

struct map_context_test : public enc_dec_map_context<string,int> {
   int index;
   size_t      operator()(size_t p,string& s, int& i) { EXPECT_EQ(index,1); return enc_dec_pair(p,s,i); }
   char *      operator()(char * p,string& s, int& i) { EXPECT_EQ(index,i); ++index; return enc_dec_pair(p,s,i); }
   const char *operator()(const char *p,string&s,int&i) {p = enc_dec_pair(p,s,i); EXPECT_EQ(index,i); ++index; return p; }
};


TEST(test_enc_dec, map_context) {
   map_context_test t;
   map<string,int> m,m2;
   t.index = 1;
   
   m["a"] = 1;
   m["b"] = 2;
   m["c"] = 3;

   char buffer[100];

   size_t sz = enc_dec(size_t(0),m,t);

   char *end = enc_dec(buffer,m,t);

   //estimation is 9 bytes more in this case. Are we okay?
   EXPECT_GT(sz,size_t(end-buffer));
   t.index = 1;

   const char *dec_end = enc_dec((const char *)buffer,m2,t);

   EXPECT_EQ(dec_end,end);

   EXPECT_EQ(m,m2);
}

struct set_context_test : public enc_dec_set_context<int> {
   int index;
   size_t      operator()(size_t p,int& i) { EXPECT_EQ(index,1); return enc_dec(p,i); }
   char *      operator()(char * p,int& i) { EXPECT_EQ(index,i); ++i; return enc_dec(p,i); }
   const char *operator()(const char *p,int&i) {p = enc_dec(p,i); EXPECT_EQ(index,i); ++index; return p; }
};


TEST(test_enc_dec, set_context) {
   set_context_test t;
   set<int> s,s2;
   t.index = 1;
   
   s.insert(1);
   s.insert(2);
   s.insert(3);

   char buffer[100];

   size_t sz = enc_dec(size_t(0),s,t);

   char *end = enc_dec(buffer,s,t);

   EXPECT_EQ(sz,size_t(end-buffer));

   const char *dec_end = enc_dec((const char *)buffer,s2,t);

   EXPECT_EQ(dec_end,end);

   EXPECT_EQ(s,s2);
}

struct vector_context_test : public enc_dec_vector_context<int> {
   int index;
   size_t      operator()(size_t p,int& i) { EXPECT_EQ(index,1); return enc_dec(p,i); }
   char *      operator()(char * p,int& i) { EXPECT_EQ(index,i); ++i; return enc_dec(p,i); }
   const char *operator()(const char *p,int&i) {p = enc_dec(p,i); EXPECT_EQ(index,i); ++index; return p; }
};


TEST(test_enc_dec, vector_context) {
   vector_context_test t;
   vector<int> s,s2;
   t.index = 1;
   
   s.push_back(1);
   s.push_back(2);
   s.push_back(3);

   char buffer[100];

   size_t sz = enc_dec(size_t(0),s,t);

   char *end = enc_dec(buffer,s,t);

   EXPECT_EQ(sz,size_t(end-buffer));

   const char *dec_end = enc_dec((const char *)buffer,s2,t);

   EXPECT_EQ(dec_end,end);

   EXPECT_EQ(s,s2);
}

TEST(test_enc_dec, lba) {
  uint64_t v[][2] = {
    /* value, bytes encoded */
    {0, 4},
    {1, 4},
    {0xff, 4},
    {0x10000, 4},
    {0x7f0000, 4},
    {0xffff0000, 4},
    {0x0fffffff, 4},
    {0x1fffffff, 5},
    {0xffffffff, 5},
    {0x3fffffff000, 4},
    {0x7fffffff000, 5},
    {0x1fffffff0000, 4},
    {0x3fffffff0000, 5},
    {0xfffffff00000, 4},
    {0x1fffffff00000, 5},
    {0x41000000, 4},
    {0, 0}
  };
  char buf[8] = { 0 };
  for (unsigned i=0; v[i][1]; ++i) {
    bzero(buf, 8);
    char *p = enc_dec_lba(buf, v[i][0]);
    cout << std::hex << v[i][0] << "\t" << v[i][1] << "\t";
    cout << std::endl;
    ASSERT_EQ(__le32(p-buf), v[i][1]);
    uint64_t u;
    enc_dec_lba((const char*)buf,u);
    ASSERT_EQ(v[i][0], u);
  }

}

TEST(test_enc_dec, varint) {
  uint32_t v[][4] = {
    /* value, varint bytes, signed varint bytes, signed varint bytes (neg) */
    {0, 1, 1, 1},
    {1, 1, 1, 1},
    {2, 1, 1, 1},
    {31, 1, 1, 1},
    {32, 1, 1, 1},
    {0xff, 2, 2, 2},
    {0x100, 2, 2, 2},
    {0xfff, 2, 2, 2},
    {0x1000, 2, 2, 2},
    {0x2000, 2, 3, 3},
    {0x3fff, 2, 3, 3},
    {0x4000, 3, 3, 3},
    {0x4001, 3, 3, 3},
    {0x10001, 3, 3, 3},
    {0x20001, 3, 3, 3},
    {0x40001, 3, 3, 3},
    {0x80001, 3, 3, 3},
    {0x7f0001, 4, 4, 4},
    {0xff00001, 4, 5, 5},
    {0x1ff00001, 5, 5, 5},
    {0xffff0001, 5, 3, 5},
    {0xffffffff, 5, 1, 5},
    {1074790401, 5, 5, 5},
    {0, 0, 0, 0}
  };
  char buf[8] = { 0 };
  for (unsigned i=0; v[i][1]; ++i) {
    {
      bzero(buf, 8);
      char *p = enc_dec_varint(buf, v[i][0]);
      cout << std::hex << v[i][0] << "\t" << v[i][1] << "\t";
      cout << std::endl;
      ASSERT_EQ(__le32(p-buf), v[i][1]);
      uint32_t u;
      enc_dec_varint((const char*)buf, u);
      ASSERT_EQ(v[i][0], u);
    }
    {
      bzero(buf, 8);
      int32_t vi = v[i][0];
      char *p = enc_dec_varint(buf, vi);
      cout << std::hex << v[i][0] << "\t" << v[i][2] << "\t";
      cout << std::endl;
      ASSERT_EQ(__le32(p-buf), v[i][2]);
      int32_t u;
      enc_dec_varint((const char*)buf, u);
      ASSERT_EQ((int32_t)v[i][0], u);
    }
    {
      bzero(buf, 8);
      int64_t x = -(int64_t)v[i][0];
      char *p = enc_dec_varint(buf, x);
      cout << std::dec << x << std::hex << "\t" << v[i][3] << "\t";
      cout << std::endl;
      ASSERT_EQ(__le32(p-buf), v[i][3]);
      int64_t u;
      enc_dec_varint((const char*)buf, u);
      ASSERT_EQ(x, u);
    }
  }
}

TEST(test_enc_dec, varint_lowz) {
  uint32_t v[][4] = {
    /* value, bytes encoded */
    {0, 1, 1, 1},
    {1, 1, 1, 1},
    {2, 1, 1, 1},
    {15, 1, 1, 1},
    {16, 1, 1, 1},
    {31, 1, 2, 2},
    {63, 2, 2, 2},
    {64, 1, 1, 1},
    {0xff, 2, 2, 2},
    {0x100, 1, 1, 1},
    {0x7ff, 2, 2, 2},
    {0xfff, 2, 3, 3},
    {0x1000, 1, 1, 1},
    {0x4000, 1, 1, 1},
    {0x8000, 1, 1, 1},
    {0x10000, 1, 2, 2},
    {0x20000, 2, 2, 2},
    {0x40000, 2, 2, 2},
    {0x80000, 2, 2, 2},
    {0x7f0000, 2, 2, 2},
    {0xffff0000, 4, 4, 4},
    {0xffffffff, 5, 5, 5},
    {0x41000000, 3, 4, 4},
    {0, 0, 0, 0}
  };
  char buf[8] = { 0 };
  for (unsigned i=0; v[i][1]; ++i) {
    {
      bzero(buf, 8);
      char *p = enc_dec_varint_lowz(buf, v[i][0]);
      cout << std::hex << v[i][0] << "\t" << v[i][1] << "\t";
      cout << std::endl;
      ASSERT_EQ(__le32(p-buf), v[i][1]);
      uint32_t u;
      enc_dec_varint_lowz((const char*)buf, u);
      ASSERT_EQ(v[i][0], u);
    }
    {
      bzero(buf, 8);
      int64_t x = v[i][0];
      char *p = enc_dec_varint_lowz(buf, x);
      cout << std::hex << x << "\t" << v[i][2] << "\t";
      cout << std::endl;
      ASSERT_EQ(__le32(p-buf), v[i][2]);
      int64_t u;
      enc_dec_varint_lowz((const char*)buf, u);
      ASSERT_EQ(x, u);
    }
    {
      bzero(buf, 8);
      int64_t x = -(int64_t)v[i][0];
      char *p = enc_dec_varint_lowz(buf, x);
      cout << std::dec << x << "\t" << v[i][3] << "\t";
      cout << std::endl;
      ASSERT_EQ(__le32(p-buf), v[i][3]);
      int64_t u;
      enc_dec_varint_lowz((const char*)buf, u);
      ASSERT_EQ(x, u);
    }
  }
}

char* gen_buffer(uint64_t size)
{
  char *buffer = new char[size];
  boost::random::random_device rand;
  rand.generate(buffer, buffer + size);
  return buffer;
}

void
generate_dummy_onode(bluestore_onode_t& onode)
{
  onode.nid = 1252;
  onode.size = 4194304;
  char *buf = gen_buffer(293);
  onode.attrs["-"] = bufferptr(buf, 293);
  char *snapset = gen_buffer(31);
  onode.attrs["snapset"] = bufferptr(snapset, 31);
  uint64_t offset = 0;
  for (int i = 1; i <= 1024; i++) {
    onode.extent_map[offset] =  bluestore_lextent_t(i, 0, 4096);
    offset += 4096;
  }
  onode.omap_head = 12;
  onode.expected_object_size = 4194304;
  onode.expected_write_size = 4194304;
  onode.alloc_hint_flags = 100;
}

void
generate_dummy_blob(bluestore_blob_t& blob)
{
  blob.init_csum(bluestore_blob_t::CSUM_XXHASH32, 16, 65536);
  char *buf = gen_buffer(8);
  blob.csum_data = buffer::claim_char(8, buf);
  blob.ref_map.get(3, 5);
  blob.add_unused(0, 3, 4096);
  blob.add_unused(8, 8, 4096);
  blob.extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  blob.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  blob.extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
}

void
generate_dummy_blob_map(BlueStore::BlobMap& map)
{
  uint64_t offset = 0xf114a5000;
  for (int i = 1; i <= 1024; i++) {
    BlueStore::Blob *b = new BlueStore::Blob();
    b->blob.init_csum(bluestore_blob_t::CSUM_CRC32C, 12, 4096);
    b->blob.csum_data = buffer::claim_malloc(8, strdup("abcdefgh"));
    b->blob.extents.emplace_back(bluestore_pextent_t(offset, 0x2000));
    b->blob.set_flag(bluestore_blob_t::FLAG_MUTABLE|bluestore_blob_t::FLAG_CSUM);
    offset += 4096;
    map.claim(b);
  }
}

void dump_onode_ondisk(bluestore_onode_t &o)
{
  std::cout << __func__ << " nid " << o.nid
                  << " size 0x" << std::hex << o.size
                  << " (" << std::dec << o.size << ")"
                  << " expected_object_size " << o.expected_object_size
                  << " expected_write_size " << o.expected_write_size
                  << std::endl;
  for (map<string,bufferptr>::iterator p = o.attrs.begin();
       p != o.attrs.end();
       ++p) {
    std::cout << __func__ << "  attr " << p->first
                    << " len " << p->second.length() << std::endl;
  }
  uint64_t pos = 0;
  for (auto& p : o.extent_map) {
    std::cout << __func__ << "  lextent 0x" << std::hex << p.first
                    << std::dec << ": " << p.second
                    << std::endl;
    assert(p.first >= pos);
    pos = p.first + p.second.length;
  }
}

TEST(test_enc_dec, lextent_enc_dec) {
   bluestore_lextent_t l1(23232, 0, 4096);
   //bluestore_lextent_t l2;
   size_t sz = enc_dec(size_t(0), l1);
   std::cout<< " lextent estimated size: " << sz << std::endl;
   char *buf = new char[sz];
   char *p = enc_dec(buf, l1);
   std::cout<< " lextent encoded length: " << int(p-buf) << std::endl;
   delete []buf;
   bufferlist bl;
   l1.encode(bl);
   std::cout<< " lextent encoded length(default): " << bl.length() << std::endl;
   //bufferlist bl;
   //std::cout << " L1 extent: " << l1 << std::endl;
   //enc(bl, l1);
   //bufferlist::iterator it = bl.begin();
   //dec(it,l2);
   //std::cout << " L2 extent: " << l2 << std::endl;
}

TEST(test_enc_dec, onode_enc_dec) {
  bluestore_onode_t onode;
  generate_dummy_onode(onode);
  bluestore_onode_t o2;
  //dump_onode_ondisk(onode);
  //bufferlist bl;
  //enc(bl, onode);
  size_t sz = enc_dec(size_t(0), onode);
  std::cout<< " Onode estimated size: " << sz << std::endl;
  char *buf = new char[sz];
  char *p = enc_dec(buf, onode);
  std::cout<< " Onode encoded length: " << int(p-buf) << std::endl;
  delete []buf;
  //bufferlist::iterator it = bl.begin();
  //dec(it,o2);
  //dump_onode_ondisk(o2);
}

void small_encode_copy(const map<uint64_t,bluestore_lextent_t>& extents, bufferlist& bl)
{
  size_t n = extents.size();
  small_encode_varint(n, bl);
  if (n) {
    auto p = extents.begin();
    small_encode_varint_lowz(p->first, bl);
    p->second.encode(bl);
    uint64_t pos = p->first;
    while (--n) {
      ++p;
      small_encode_varint_lowz((uint64_t)p->first - pos, bl);
      p->second.encode(bl);
      pos = p->first;
    }
  }
}

struct extent_map_context  : public enc_dec_map_context<uint64_t,bluestore_lextent_t> {
   uint64_t pos = 0;

   virtual size_t  operator() (size_t p, uint64_t& off, bluestore_lextent_t& le)
   {
      return  enc_dec_pair(p,off,le); // need blob + lextent encoding estimate
   }

   virtual char* operator() (char *p, uint64_t& off, bluestore_lextent_t &le)
   {
     uint64_t offset = off-pos;
     pos = off;
     p = enc_dec_varint_lowz(p, offset);
     p = enc_dec(p, le);
     return p;
   }

   virtual const char* operator() (const char *p,uint64_t &off, bluestore_lextent_t& le)
   {
     uint64_t delta;
     p = enc_dec_varint_lowz(p, delta);
     pos += delta;
     off = pos;
     p = enc_dec(p, le);
     return p;
   }
};

TEST(test_enc_dec, extent_map) {
  map<uint64_t,bluestore_lextent_t> extent_map;
  uint64_t offset = 0;
  for (int i = 1; i <= 512; i++) {
    extent_map[offset] =  bluestore_lextent_t(i, 0, 8192);
    offset += 8192;
  }
  size_t sz = enc_dec(size_t(0), extent_map);
  std::cout<< " extent map estimated size: " << sz << std::endl;
  char *buf = new char[sz];
  char *p = enc_dec(buf, extent_map);
  std::cout<< " extent map encoded length: " << int(p-buf) << std::endl;
  delete []buf;
  bufferlist bl;
  small_encode_copy(extent_map, bl);
  std::cout << "extent map encoded length(default):" << bl.length() << std::endl;
}

TEST(test_enc_dec, extent_map_context) {
  map<uint64_t,bluestore_lextent_t> extent_map;
  uint64_t offset = 0;
  for (int i = 1; i <= 512; i++) {
    extent_map[offset] =  bluestore_lextent_t(i, 0, 8192);
    offset += 8192;
  }
  extent_map_context ctx;
  size_t sz = enc_dec(size_t(0), extent_map, ctx);
  std::cout<< " extent map estimated size: " << sz << std::endl;
  char *buf = new char[sz];
  char *p = enc_dec(buf, extent_map, ctx);
  std::cout<< " extent map encoded length: " << int(p-buf) << std::endl;
  delete []buf;
  bufferlist bl;
  small_encode_copy(extent_map, bl);
  std::cout << "extent map encoded length(default):" << bl.length() << std::endl;
}

TEST(test_enc_dec, blob_map) {
  BlueStore::BlobMap bmap;
  generate_dummy_blob_map(bmap);
  size_t sz = enc_dec(size_t(0), bmap);
  std::cout<< " extent map estimated size: " << sz << std::endl;
  char *buf = new char[sz];
  char *p = enc_dec(buf, bmap);
  std::cout<< " Blob map encoded length: " << int(p-buf) << std::endl;
  bufferlist bl;
  bmap.encode(bl);
  std::cout << "blob map encoded length(default):" << bl.length() << std::endl;
}

struct onode_blob_map_context  : public enc_dec_map_context<uint64_t,bluestore_lextent_t> {
   BlueStore::BlobMap& blob_map;
   BlueStore::BlobMap temp_blob_map;
   int new_blob_id = 1;
   uint64_t pos = 0;
   map<int, int> old_new; // key is "old" blob_id, value is 'new' blob_id;
   uint8_t csum_type = -1;
   uint8_t csum_chunk_order = -1;
   uint32_t le_length = 0;

   onode_blob_map_context(BlueStore::BlobMap& _map):blob_map(_map) { }
   void setbit_location(uint8_t &n, int pos){
     n |= 1 << pos;
   }

   void clearbit_location(uint8_t &n, int pos){
     n &= ~(1 << pos);
   }

   bool is_set(uint8_t &n, int pos){
     return n & (1 << pos);
   }

   virtual size_t operator() (size_t p, uint64_t& off, bluestore_lextent_t& le)
   {
      return  enc_dec_pair(p,off,le); // need blob + lextent encoding estimate
   }

   virtual char* operator() (char *p, uint64_t& off, bluestore_lextent_t &le)
   {
      // From LSB to MSB
      //  1. offset is not present in lextent or starts with a zero
      //  2. blob has single pextent vector
      //  3. lextent length is same as blob length
      //  4. csum_type is same as previous one
      //  5. csum_order is same as previous one
      //  6. lextent length is same as previous one
     uint8_t optimize = 0;
     if (le.offset == 0) {
       //set the LSB bit to indicate offset starts from 0
       setbit_location(optimize, 0);
     }
     p = enc_dec(p, optimize);
     uint64_t offset = off-pos;
     pos = off;
     p = enc_dec_varint_lowz(p, offset);
     if (le.offset == 0) {
       p = enc_dec_varint(p, le.blob);
       if (le_length == le.length) {
         setbit_location(optimize, 5);
       } else {
         p = enc_dec_varint_lowz(p, le.length);
       }
     } else {
       p = enc_dec(p, le);
     }
     le_length = le.length;
     int b = le.blob;
     if (b < 0) { // global blob id, serialize after the lextent serialization
     } else if (old_new.find(b) == old_new.end()) { // New blob, serialize along with lextent
       BlueStore::BlobRef blob = blob_map.get(b);
       p = enc_dec_varint(p, new_blob_id);
       p = enc_dec(p, blob->blob);
       old_new[le.blob] = new_blob_id++;
       // we have to serialize the pextents separately
       // if it has a single extent, encode the extent not going through vector
       // if it has single extent and length is same as lextent length don't
       // encode, else go through vector context
       if (blob->blob.extents.size() == 1) {
          setbit_location(optimize, 1);
	  p = enc_dec_lba(p, blob->blob.extents.front().offset);
	  if (blob->blob.extents.front().length == le.length) {
             setbit_location(optimize, 2);
	  } else {
	    p = enc_dec_varint_lowz(p, blob->blob.extents.front().length);
	  }
       } else {
	 p = enc_dec(p, blob->blob.extents);
       }
       if (blob->blob.has_csum()) {
          // handle csum_type
          if (blob->blob.csum_type == csum_type) {
             setbit_location(optimize, 3);
          } else {
             p = enc_dec(p, blob->blob.csum_type);
          }

         //handle csum_order
          if (blob->blob.csum_chunk_order == csum_chunk_order) {
             setbit_location(optimize, 4);
          } else {
             p = enc_dec(p, blob->blob.csum_chunk_order);
         }

          p = enc_dec(p, blob->blob.csum_data);
       }

       // store the previous blob attrs
       csum_type = blob->blob.csum_type;
       csum_chunk_order = blob->blob.csum_chunk_order;


     } else { // already serialized, let us remember the id
       auto old_blob_id = old_new.find(b);
       int blob_id = old_blob_id->first;
       p = enc_dec_varint(p, blob_id);
     }
     return p;
   }

   virtual const char* operator() (const char *p,uint64_t &off, bluestore_lextent_t& le)
   {
     uint8_t optimize = 0;
     p = enc_dec(p, optimize);
     bool off_zero =  is_set(optimize, 0);
     int b;
     uint64_t delta;
     p = enc_dec_varint_lowz(p, delta);
     pos += delta;
     off = pos;
     if (off_zero) {
       p = enc_dec_varint(p, le.blob);
       p = enc_dec_varint_lowz(p, le.length);
       le.offset = 0;
     } else {
       p = enc_dec(p, le);
     }
     p = enc_dec(p, le);
     p = enc_dec_varint(p, b);
     if (b < 0) {
       //nothing to do , we will deserialize  actual blob later
     } else if (old_new.find(b) == old_new.end()) {
       // blob id has to be retained
       //BlueStore::Blob* blob = new BlueStore::Blob;
       bluestore_blob_t blob;
       p = enc_dec(p, blob);
       if (is_set(optimize, 1)) {
	 bluestore_pextent_t pe;
	 p = enc_dec_lba(p, pe.offset);
	 if (!is_set(optimize, 2)) {
	   p = enc_dec_varint_lowz(p, pe.length);
	 }
	 blob.extents.emplace_back(pe);
       } else {
	 p = enc_dec(p, blob.extents);
       }
       old_new[b] = new_blob_id++;
       BlueStore::Blob* b = new BlueStore::Blob(le.blob, NULL);
       b->blob = blob;
       // is this a memory leak here? need to handle
       blob_map.blob_map.insert(*b);
     } else {
      assert(old_new.find(b) != old_new.end());
     }
     return p;
   }
};

void onode_blob_encode(bluestore_onode_t& o, BlueStore::BlobMap& bmap)
{
  bufferlist bl;
  onode_blob_map_context t(bmap);
  size_t sz = enc_dec(size_t(0), o);
  sz += enc_dec(size_t(0), o.extent_map, t);
  sz += enc_dec(size_t(0), bmap);
  std::cout << __func__ << o.nid << " estimated size: " << sz << std::endl;
  char *buffer = new char[sz];
  char *data_start = buffer + sizeof(__le32);
  char *end = enc_dec(data_start, o);
  std::cout << __func__ << " encoded size of onode: " << (int)(end-data_start) << std::endl;
  end = enc_dec(end, o.extent_map, t);
  std::cout << __func__ << " encoded size of onode_blob_map: " << (int)(end-buffer) << std::endl;
  //TODO:: can we optimize this? ideally we shouldn't have any blobs other
  //than onode space ones. Is there anything missing?
  if (bmap.blob_map.size() > t.old_new.size()) {
    BlueStore::BlobMap temp_blob_map; // key is "old" blob_id, value is 'new' blob_id;
    for (auto& p : bmap.blob_map) {
      if(t.old_new.find(p.id) == t.old_new.end()) {
	temp_blob_map.blob_map.insert(p);
      }
    }
    end = enc_dec(end, temp_blob_map);
  } else {
    uint16_t sz = bmap.blob_map.size() - t.old_new.size();
    end = enc_dec(end, sz);
  }
  std::cout << __func__ << o.nid << " encoded size : " << (int)(end-buffer) << std::endl;
  *(__le32*)buffer= __le32(end-data_start);
  //size_t act_size = (__le32 )(end-data_start);
  //std::cout << __func__ << " actual size: " << act_size << std::endl;
  //TODO:: use pop back to release the unsed buffer back, but that seems broken now
  // we are consuming more than actual encoding now, have to debug the size
  //bufferptr bp = buffer::claim_char(sz, buffer);
  //bl.push_back(bp);
  //std::cout << __func__ << "bufferlist size: " << bl.length() << std::endl;

  // TODO:: Take this to mainsteam code
  // testing decode
  //onode_blob_decode(bl);
  //bl.clear();
  delete []buffer;
}

void dump_blob_map_decode(BlueStore::BlobMap &bm)
{
  for (auto& b : bm.blob_map) {
    std::cout << __func__ << "  " << b << std::endl;
    if (b.blob.has_csum()) {
      vector<uint64_t> v;
      unsigned n = b.blob.get_csum_count();
      for (unsigned i = 0; i < n; ++i)
	v.push_back(b.blob.get_csum_item(i));
      std::cout << __func__ << "       csum: " << std::hex << v << std::dec
		      << std::endl;
    }
  }
}

void onode_blob_decode(bufferlist& bl)
{
  bufferlist::iterator i = bl.begin();
  unique_ptr<char> buf0;
  unique_ptr<char> buf1; // Incase we have to allocate a buffer
  bluestore_onode_t o2;
  map<uint64_t, bluestore_lextent_t> le_map;
  BlueStore::BlobMap map2, map3;
  //
  // Read the sentinal
  //
  const char *dec_buf_start = straighten_iterator(i,sizeof(__le32),buf0);
  size_t buf_sz = *(__le32 *)dec_buf_start;
  //
  // Now the data itself
  //
  const char *dec_data_start  = straighten_iterator(i,buf_sz,buf1);
  //std::cout << __func__ << "decode actual size: " << buf_sz << std::endl;
  onode_blob_map_context t2(map2);
  const char *dec_end = enc_dec((const char *)dec_data_start, o2);
  dec_end = enc_dec((const char *)dec_end, le_map, t2);
  __le16 map_size;
  dec_end = enc_dec((const char *)dec_end, map_size);
  //std::cout << __func__ << "Decoded map size: " << map_size << std::endl;
  if (map_size > 0) {
    dec_end -= sizeof(__le16);
    dec_end = enc_dec((const char *)dec_end, map3);
  }
  std::cout << __func__ << " Decoded size: " << (int) (dec_end - dec_data_start) << std::endl;
  //dump_onode_ondisk(o2);
  o2.extent_map = le_map;
  //dump_onode_ondisk(o2);
  //dump_blob_map_decode(t2.blob_map);
  //for (auto& p : le_map) {
   // std::cout << __func__ << "  lextent 0x" << std::hex << p.first
     //               << std::dec << ": " << p.second
     //               << std::endl;
  //}
  //dump_blob_map_decode(map3, 0);
}

TEST(test_enc_dec, onode_blob_map) {
  bluestore_onode_t onode;
  generate_dummy_onode(onode);
  //bluestore_onode_t o2;
  BlueStore::BlobMap bmap;
  generate_dummy_blob_map(bmap);
  onode_blob_encode(onode, bmap);
  bufferlist bl;
  onode.encode(bl);
  std::cout << "onode encoded length(default):" << bl.length() << std::endl;
  bmap.encode(bl);
  std::cout << "blob map encoded length(default):" << bl.length() << std::endl;
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


/*
 * Local Variables:
 * compile-command: "cd .. ; make -j4 &&
 *   make unittest_enc_dec &&
 *   valgrind --tool=memcheck ./unittest_enc_dec --gtest_filter=*.*"
 * End:
 */
