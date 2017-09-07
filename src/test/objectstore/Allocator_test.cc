// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In memory space allocator test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */
#include <iostream>
//#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/Context.h"
#include "os/bluestore/Allocator.h"
#include "os/bluestore/BitAllocator.h"
#include <thread>
using namespace std;


#if GTEST_HAS_PARAM_TEST

class AllocTest : public ::testing::TestWithParam<const char*> {
public:
    std::shared_ptr<Allocator> alloc;
    AllocTest(): alloc(nullptr) { }
    void init_alloc(int64_t size, uint64_t min_alloc_size) {
      std::cout << "Creating alloc type " << string(GetParam()) << " \n";
      alloc.reset(Allocator::create(g_ceph_context, string(GetParam()), size,
				    min_alloc_size));
    }

    void init_close() {
      alloc.reset();
    }
};

TEST_P(AllocTest, test_alloc_init)
{
  int64_t blocks = BmapEntry::size();
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = BitMapZone::get_total_blocks() * 2 + 16;
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = BitMapZone::get_total_blocks() * 2;
  init_alloc(blocks, 1);
  ASSERT_EQ(alloc->get_free(), (uint64_t) 0);
}

TEST_P(AllocTest, test_alloc_min_alloc)
{
  int64_t block_size = 1024;
  int64_t blocks = BitMapZone::get_total_blocks() * 2 * block_size;
  std::cout << __func__ << " blocks: " << blocks << std::endl;

  {
    init_alloc(blocks, block_size);
    alloc->init_add_free(block_size, block_size);
    EXPECT_EQ(alloc->reserve(block_size), 0);
    AllocExtentVector extents;
    EXPECT_EQ(block_size, alloc->allocate(block_size, block_size,
					  0, (int64_t) 0, &extents));
  }

  /*
   * Allocate extent and make sure all comes in single extent.
   */   
  {
    alloc->init_add_free(0, block_size * 4);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    AllocExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      0, (int64_t) 0, &extents));
    EXPECT_EQ(1u, extents.size());
    EXPECT_EQ(extents[0].length, 4 * block_size);
  }

  /*
   * Allocate extent and make sure we get two different extents.
   */
  {
    alloc->init_add_free(0, block_size * 2);
    alloc->init_add_free(3 * block_size, block_size * 2);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    AllocExtentVector extents;
  
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      0, (int64_t) 0, &extents));
    EXPECT_EQ(2u, extents.size());
    EXPECT_EQ(extents[0].length, 2 * block_size);
    EXPECT_EQ(extents[1].length, 2 * block_size);
  }
  alloc->shutdown();
}

TEST_P(AllocTest, test_alloc_min_max_alloc)
{
  int64_t block_size = 1024;
  int64_t blocks = BitMapZone::get_total_blocks() * 2 * block_size;

  init_alloc(blocks, block_size);

  /*
   * Make sure we get all extents different when
   * min_alloc_size == max_alloc_size
   */
  {
    alloc->init_add_free(0, block_size * 4);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    AllocExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      block_size, (int64_t) 0, &extents));
    for (auto e : extents) {
      EXPECT_EQ(e.length, block_size);
    }
    EXPECT_EQ(4u, extents.size());
  }


  /*
   * Make sure we get extents of length max_alloc size
   * when max alloc size > min_alloc size
   */
  {
    alloc->init_add_free(0, block_size * 4);
    EXPECT_EQ(alloc->reserve(block_size * 4), 0);
    AllocExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      2 * block_size, (int64_t) 0, &extents));
    EXPECT_EQ(2u, extents.size());
    for (auto& e : extents) {
      EXPECT_EQ(e.length, block_size * 2);
    }
  }

  /*
   * Make sure allocations are of min_alloc_size when min_alloc_size > block_size.
   */
  {
    alloc->init_add_free(0, block_size * 1024);
    EXPECT_EQ(alloc->reserve(block_size * 1024), 0);
    AllocExtentVector extents;
    EXPECT_EQ(1024 * block_size,
	      alloc->allocate(1024 * (uint64_t)block_size,
			      (uint64_t) block_size * 4,
			      block_size * 4, (int64_t) 0, &extents));
    for (auto& e : extents) {
      EXPECT_EQ(e.length, block_size * 4);
    }
    EXPECT_EQ(1024u/4, extents.size());
  }

  /*
   * Allocate and free.
   */
  {
    alloc->init_add_free(0, block_size * 16);
    EXPECT_EQ(alloc->reserve(block_size * 16), 0);
    AllocExtentVector extents;
    EXPECT_EQ(16 * block_size,
	      alloc->allocate(16 * (uint64_t)block_size, (uint64_t) block_size,
			      2 * block_size, (int64_t) 0, &extents));

    EXPECT_EQ(extents.size(), 8u);
    for (auto& e : extents) {
      EXPECT_EQ(e.length, 2 * block_size);
      alloc->release(e.offset, e.length);
    }
  }
}

TEST_P(AllocTest, test_alloc_failure)
{
  int64_t block_size = 1024;
  int64_t blocks = BitMapZone::get_total_blocks() * block_size;

  init_alloc(blocks, block_size);
  {
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);

    EXPECT_EQ(alloc->reserve(block_size * 512), 0);
    AllocExtentVector extents;
    EXPECT_EQ(512 * block_size,
	      alloc->allocate(512 * (uint64_t)block_size,
			      (uint64_t) block_size * 256,
			      block_size * 256, (int64_t) 0, &extents));
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);
    extents.clear();
    EXPECT_EQ(alloc->reserve(block_size * 512), 0);
    EXPECT_EQ(-ENOSPC,
	      alloc->allocate(512 * (uint64_t)block_size,
			      (uint64_t) block_size * 512,
			      block_size * 512, (int64_t) 0, &extents));
  }
}

TEST_P(AllocTest, test_alloc_big)
{
  int64_t block_size = 4096;
  int64_t blocks = 104857600;
  int64_t mas = 4096;
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(2*block_size, (blocks-2)*block_size);
  for (int64_t big = mas; big < 1048576*128; big*=2) {
    cout << big << std::endl;
    EXPECT_EQ(alloc->reserve(big), 0);
    AllocExtentVector extents;
    EXPECT_EQ(big,
	      alloc->allocate(big, mas, 0, &extents));
  }
}

TEST_P(AllocTest, test_alloc_hint_bmap)
{
  if (GetParam() == std::string("stupid")) {
    return;
  }
  int64_t blocks = BitMapArea::get_level_factor(g_ceph_context, 2) * 4;
  int64_t allocated = 0;
  int64_t zone_size = 1024;
  g_conf->set_val("bluestore_bitmapallocator_blocks_per_zone",
		  std::to_string(zone_size));

  init_alloc(blocks, 1);
  alloc->init_add_free(0, blocks);

  AllocExtentVector extents;
  alloc->reserve(blocks);

  allocated = alloc->allocate(1, 1, 1, zone_size, &extents);
  ASSERT_EQ(1, allocated);
  ASSERT_EQ(1u, extents.size());
  ASSERT_EQ(extents[0].offset, (uint64_t) zone_size);

  extents.clear();
  allocated = alloc->allocate(1, 1, 1, zone_size * 2 - 1, &extents);
  EXPECT_EQ(1, allocated);
  ASSERT_EQ(1u, extents.size());
  EXPECT_EQ((int64_t) extents[0].offset, zone_size * 2 - 1);

  /*
   * Wrap around with hint
   */
  extents.clear();
  allocated = alloc->allocate(zone_size * 2, 1, 1,  blocks - zone_size * 2,
			      &extents);
  ASSERT_EQ(zone_size * 2, allocated);
  EXPECT_EQ(zone_size * 2, (int)extents.size());
  EXPECT_EQ((int64_t)extents[0].offset, blocks - zone_size * 2);

  extents.clear();
  allocated = alloc->allocate(zone_size, 1, 1, blocks - zone_size, &extents);
  ASSERT_EQ(zone_size, allocated);
  EXPECT_EQ(zone_size, (int)extents.size());
  EXPECT_EQ(extents[0].offset, (uint64_t) 0);
  /*
   * Verify out-of-bound hint
   */
  extents.clear();
  allocated = alloc->allocate(1, 1, 1, blocks, &extents);
  ASSERT_EQ(1, allocated);
  EXPECT_EQ(1, (int)extents.size());

  extents.clear();
  allocated = alloc->allocate(1, 1, 1, blocks * 3 + 1 , &extents);
  ASSERT_EQ(1, allocated);
  EXPECT_EQ(1, (int)extents.size());
}

TEST_P(AllocTest, test_alloc_non_aligned_len)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 100;
  int64_t want_size = 1 << 22;
  int64_t alloc_unit = 1 << 20;
  
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(0, 2097152);
  alloc->init_add_free(2097152, 1064960);
  alloc->init_add_free(3670016, 2097152);

  EXPECT_EQ(0, alloc->reserve(want_size));
  AllocExtentVector extents;
  EXPECT_EQ(want_size, alloc->allocate(want_size, alloc_unit, 0, &extents));
}

#define NUM_ALLOCATORS 4
#define NUM_SCAVENGERS 1

vector<AllocExtentVector> allocs[NUM_ALLOCATORS];
int64_t block_size = 4096;
bool stop = false;
std::mutex v_lock;

void do_join(std::thread& t)
{
    t.join();
}

void join_all(std::vector<std::thread>& v)
{
    std::for_each(v.begin(),v.end(),do_join);
}

void allocate_space(int index, std::shared_ptr<Allocator> alloc)
{
//going to allocate space based on index
    AllocExtentVector extents;
    int failures = 0;
    while(1) {

	    //std::cout << "Allocating at index: " << index << std::endl;
	    extents.clear();
	    size_t size = (index+1) * block_size;
  	    alloc->reserve(size);
	    alloc->allocate(size,
			    (uint64_t) block_size,
			    (uint64_t) block_size,
			    (int64_t) 0,
			    &extents);
	   if (extents.size() == 0) {
	      std::cout << " Allocation failed for index: " << index << " waiting..." << std::endl;
	      failures++;
	      std::this_thread::sleep_for(std::chrono::milliseconds(4));
	   } else {
	    for (auto& e : extents) {
		assert(!(e.offset%block_size));
	    }
           }
	   {
		std::unique_lock<std::mutex> l(v_lock);
	   	allocs[index].push_back(extents);
	   }
	   if (failures == 5) break;
	   if (stop) break;
   }

}

void release_space(std::shared_ptr<Allocator> alloc)
{
//going to allocate space based on index
    AllocExtentVector extents;
    int iters = NUM_ALLOCATORS;
    while(1) {
	for (int i = 0; i < iters; i++) {
	   //std::cout << "Freeing at index: " << i << std::endl;
	   {
		std::unique_lock<std::mutex> l(v_lock);
	   	extents = allocs[i].front();
	   }
	   if (extents.size() > 0) {
		for (auto& e : extents) {
			alloc->release(e.offset, e.length);
		}
		extents.clear();
		{
			std::unique_lock<std::mutex> l(v_lock);
			allocs[i].erase(allocs[i].begin());
		}
	   } else {
		   //remove from alloc vector
		   //have to adjust the num allocatos in that case dynamically
		   std::unique_lock<std::mutex> l(v_lock);
		   iters--;
		   allocs.erase(allocs.begin() + i);
	   }

	}
	if (stop) break;
   }

}

TEST_P(AllocTest, test_concurrent)
{
  uint64_t size = 1*1024*1024*1024*1024ULL; 
  std::cout << __func__ << " size: " << size << std::endl;
  init_alloc(size, block_size);
  alloc->init_add_free(0, size);
  EXPECT_EQ(alloc->reserve(size), 0);
  {
    std::vector<std::thread> allocator_threads;
    for (int i=0; i<NUM_ALLOCATORS; i++) {
      allocator_threads.push_back(std::thread(allocate_space, i, std::ref(alloc)));
    }

    std::vector<std::thread> release_threads;
    for (int i=0; i<NUM_SCAVENGERS; i++) {
      release_threads.push_back(std::thread(release_space, std::ref(alloc) ));
    }
    sleep(300);
    stop = true;

    join_all(allocator_threads);
    join_all(release_threads);
  }
}

INSTANTIATE_TEST_CASE_P(
  Allocator,
  AllocTest,
  ::testing::Values("stupid", "bitmap"));

#else

TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}
#endif
