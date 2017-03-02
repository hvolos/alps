/* 
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fcntl.h>

#include <cinttypes>

#include "gtest/gtest.h"
#include "alps/layers/pointer.hh"
#include "alps/layers/extentheap.hh"

#include "allochelper.hh"
#include "test_common.hh"

using namespace alps;

typedef nvExtentHeap<TPtr, PPtr> nvExtentHeap_t;

typedef ExtentHeap<TPtr, PPtr> ExtentHeap_t;
typedef Extent<TPtr, PPtr> Extent_t;

size_t region_size = 1024*1024;
size_t block_log2size = 12; // 4KB

TEST(ExtentHeapTest, create)
{
    TPtr<void> region = malloc(region_size);

    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
    EXPECT_NE((void*) 0, exheap);
}

TEST(ExtentHeapTest, alloc)
{
    TPtr<void> region = malloc(region_size);

    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
    Extent_t ex;
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(10, &ex));
}

TEST(ExtentHeapTest, alloc_free)
{
    TPtr<void> region = malloc(region_size);

    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
    Extent_t ex[16];
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(10, &ex[0]));
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(1, &ex[1]));
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(14, &ex[2]));
    for (int i = 0; i<3; i++) {
        EXPECT_EQ(kErrorCodeOk, exheap->free_extent(ex[i]));
    }
}

TEST(ExtentHeapTest, iterator)
{
    TPtr<void> region = malloc(region_size);

    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
    Extent_t ex[8];
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(10, &ex[0]));
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(3, &ex[1]));
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(14, &ex[2]));

    EXPECT_EQ(kErrorCodeOk, exheap->free_extent(ex[1]));
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(228, &ex[1]));
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(1, &ex[4]));

    ExtentHeap_t::Iterator it;
    for (it = exheap->begin(); it != exheap->end(); ++it) {
        if (!(*it).nvheader()->is_free()) {
            bool found = false;
            for (int i=0; i<8; i++) {
                if (ex[i] == (*it)) {
                    found = true;
                    break;
                }
            }
            EXPECT_EQ(true, found);
        }
    }
}

class ExtentHeapWrapper {
public:

    ExtentHeapWrapper(ExtentHeap_t* exheap)
        : exheap_(exheap)
    { }

    TPtr<void> malloc(size_t size) 
    {
        Extent_t ex;
        size_t nblocks = size / exheap_->blocksize();
        exheap_->alloc_extent(nblocks, &ex);
        return ex.nvextent();
    }

    void free(TPtr<void> ptr)
    {
        ErrorCode rc = exheap_->free_extent(ptr);
        assert(kErrorCodeOk == rc);
    }
    
private:
    ExtentHeap_t* exheap_;

};


TEST(ExtentHeapTest, alloc_free_random)
{
    TPtr<void> region = malloc(region_size);

    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
    ExtentHeapWrapper heap(exheap);

    UniformDistribution block_dist(4096, 65536, 4096);

    random_alloc_free(&heap, block_dist, 1, 8, 16, 128, 50, true, 0x1);
}



TEST(ExtentHeapTest, load)
{
    TPtr<void> region = malloc(region_size);

    // create heap and do some allocations
    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
    Extent_t ex;
    EXPECT_EQ(kErrorCodeOk, exheap->alloc_extent(10, &ex));

    // reload heap and do checks
    ExtentHeap_t* exheapb = ExtentHeap_t::load(region);

    Extent_t exb;
    exheapb->extent(ex.interval(), &exb);
    EXPECT_EQ(0, exb.nvheader()->is_free());
}



int main(int argc, char** argv)
{
    ::alps::init_test_env<::alps::TestEnvironment>(argc, argv);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
