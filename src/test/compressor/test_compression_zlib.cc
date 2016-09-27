// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Alyona Kiseleva <akiselyova@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "compressor/zlib/ZlibCompressor.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"

TEST(ZlibCompressor, compress_decompress)
{
  ZlibCompressor sp(false);
  EXPECT_STREQ(sp.get_type().c_str(), "zlib");
  const char* test = "This is test text";
  int res;
  int len = strlen(test);
  bufferlist in, out;
  bufferlist after;
  bufferlist exp;
  in.append(test, len);
  res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  res = sp.decompress(out, after);
  EXPECT_EQ(res, 0);
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
  after.clear();
  size_t compressed_len = out.length();
  out.append_zero(12);
  auto it = out.begin();
  res = sp.decompress(it, compressed_len, after);
  EXPECT_EQ(res, 0);
  EXPECT_TRUE(exp.contents_equal(after));

  //large block and non-begin iterator for continuous block
  std::string data;
  data.resize(0x10000 * 1);
  for(size_t i = 0; i < data.size(); i++)
    data[i] = i / 256;
  in.clear();
  out.clear();
  in.append(data);
  exp = in;
  res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  compressed_len = out.length();
  out.append_zero(0x10000 - out.length());
  after.clear();
  out.c_str();
  bufferlist prefix;
  prefix.append(string("some prefix"));
  size_t prefix_len = prefix.length();
  out.claim_prepend(prefix);
  it = out.begin();
  it.advance(prefix_len);
  res = sp.decompress(it, compressed_len, after);
  EXPECT_EQ(res, 0);
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST(ZlibCompressor, compress_decompress_chunk)
{
  ZlibCompressor sp(false);
  EXPECT_STREQ(sp.get_type().c_str(), "zlib");
  const char* test = "This is test text";
  buffer::ptr test2 ("1234567890", 10);
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  in.append(test2);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = sp.decompress(out, after);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append("This is test text1234567890");
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST(ZlibCompressor, compress_decompress_isal)
{
  ZlibCompressor sp(true);
  EXPECT_STREQ(sp.get_type().c_str(), "zlib");
  const char* test = "This is test text";
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = sp.decompress(out, after);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
  after.clear();
  size_t compressed_len = out.length();
  out.append_zero(12);
  auto it = out.begin();
  res = sp.decompress(it, compressed_len, after);
  EXPECT_EQ(res, 0);
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST(ZlibCompressor, compress_decompress_chunk_isal)
{
  ZlibCompressor sp(true);
  EXPECT_STREQ(sp.get_type().c_str(), "zlib");
  const char* test = "This is test text";
  buffer::ptr test2 ("1234567890", 10);
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  in.append(test2);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = sp.decompress(out, after);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append("This is test text1234567890");
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST(ZlibCompressor, zlib_isal_compatibility)
{
  ZlibCompressor isal(true);
  EXPECT_STREQ(isal.get_type().c_str(), "zlib");
  ZlibCompressor zlib(false);
  EXPECT_STREQ(zlib.get_type().c_str(), "zlib");
  char test[101];
  srand(time(0));
  for (int i=0; i<100; ++i)
    test[i] = 'a' + rand()%26;
  test[100] = '\0';
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  // isal -> zlib
  int res = isal.compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = zlib.decompress(out, after);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
  after.clear();
  out.clear();
  exp.clear();
  // zlib -> isal
  res = zlib.compress(in, out);
  EXPECT_EQ(res, 0);
  res = isal.decompress(out, after);
  EXPECT_EQ(res, 0);
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
}

void test_compress(size_t size)
{
  ZlibCompressor sp(false);
  EXPECT_STREQ(sp.get_type().c_str(), "zlib");
  char* data = (char*) malloc(size);
  for (size_t t = 0; t < size; t++) {
    data[t] = (t & 0xff) | (t >> 8);
  }
  bufferlist in;
  in.append(data, size);
  for (size_t t = 0; t < 100000; t++) {
    bufferlist out;
    int res = sp.compress(in, out);
    EXPECT_EQ(res, 0);
  }
}

void test_decompress(size_t size)
{
  ZlibCompressor sp(false);
  EXPECT_STREQ(sp.get_type().c_str(), "zlib");
  char* data = (char*) malloc(size);
  for (size_t t = 0; t < size; t++) {
    data[t] = (t & 0xff) | (t >> 8);
  }
  bufferlist in, out;
  in.append(data, size);
  int res = sp.compress(in, out);
  EXPECT_EQ(res, 0);
  for (size_t t = 0; t < 100000; t++) {
    bufferlist out_dec;
    int res = sp.decompress(out, out_dec);
    EXPECT_EQ(res, 0);
  }
}

TEST(ZlibCompressor, compress_1024)
{
  test_compress(1024);
}

TEST(ZlibCompressor, compress_2048)
{
  test_compress(2048);
}

TEST(ZlibCompressor, compress_4096)
{
  test_compress(4096);
}

TEST(ZlibCompressor, compress_8192)
{
  test_compress(8192);
}

TEST(ZlibCompressor, compress_16384)
{
  test_compress(16384);
}

TEST(Zlibdecompressor, decompress_1024)
{
  test_decompress(1024);
}

TEST(Zlibdecompressor, decompress_2048)
{
  test_decompress(2048);
}

TEST(Zlibdecompressor, decompress_4096)
{
  test_decompress(4096);
}

TEST(Zlibdecompressor, decompress_8192)
{
  test_decompress(8192);
}

TEST(Zlibdecompressor, decompress_16384)
{
  test_decompress(16384);
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 && 
 *   make unittest_compression_zlib && 
 *   valgrind --tool=memcheck \
 *      ./unittest_compression_zlib \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
