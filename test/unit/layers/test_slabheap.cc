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
#include <sstream>

#include "gtest/gtest.h"
#include "alps/common/assorted_func.hh"

#include "alps/layers/bits/slab.hh"
#include "alps/layers/pointer.hh"
#include "alps/layers/slabheap.hh"

#include "test_common.hh"

using namespace alps;

class Context {

};

typedef nvSlab<Context,TPtr> nvSlab_t;

typedef Slab<Context, TPtr, PPtr> Slab_t;

typedef SlabHeap<Context, TPtr, PPtr> SlabHeap_t;


// Test SlabHeap functionality with no extent heap and zone heap for 
// allocating space for non-volatile slabs. Instead provide a method 
// that mocks allocation of non-volatile slabs.
template<template<typename> class TPtr>
class SlabHeapTestT: public ::testing::Test {
    const size_t slab_size = 256*1024;  
public:
    template<typename T>
    TPtr<T> alloc(size_t size)
    {
        return (T*) malloc(size);
    }

    TPtr<nvSlab_t> alloc_nvslab(Context& ctx, int block_sizeclass, unsigned int perc_full) {
        TPtr<nvSlab_t> nvslab = alloc<nvSlab_t>(slab_size);
        nvSlab_t::make(nvslab, slab_size, block_sizeclass);
        for (size_t i=0, nalloc=0; i<nvslab->nblocks(); i++) {
            if (100*nalloc/nvslab->nblocks() < perc_full) {
                nvslab->set_alloc(ctx, i);
                nalloc++;
            } else {
                break;
            }
        }
        return nvslab;
    }
};

typedef SlabHeapTestT<TPtr> SlabHeapTest;

TEST_F(SlabHeapTest, insert)
{
    Context ctx;
    SlabHeap_t slabheap;

    Slab_t* slab0 = slabheap.insert_slab(alloc_nvslab(ctx, 71, 0));
    Slab_t* slab1 = slabheap.insert_slab(alloc_nvslab(ctx, 71, 1));
    Slab_t* slab2 = slabheap.insert_slab(alloc_nvslab(ctx, 71, 50));
    Slab_t* slab3 = slabheap.insert_slab(alloc_nvslab(ctx, 71, 99));

    UNUSED_ND(slab0);
    UNUSED_ND(slab1);
    UNUSED_ND(slab2);

    Slab_t* slab4 = slabheap.find_slab(71);
    
    EXPECT_EQ(slab3, slab4);
    
    std::cout << slabheap << std::endl; 

    slabheap.alloc_block(ctx, slab4);
    slabheap.alloc_block(ctx, slab4);
    slabheap.alloc_block(ctx, slab4);
    
    std::cout << slabheap << std::endl; 
} 


int main(int argc, char** argv)
{
    ::alps::init_test_env<::alps::TestEnvironment>(argc, argv);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
