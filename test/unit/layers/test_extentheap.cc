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
#include "gtest/gtest.h"
#include "alps/layers/pointer.hh"
#include "alps/layers/extentheap.hh"

using namespace alps;

typedef nvExtentHeap<TPtr, PPtr> nvExtentHeap_t;

typedef ExtentHeap<TPtr, PPtr> ExtentHeap_t;

size_t region_size = 1024*1024;
size_t block_log2size = 12; // 4KB

TEST(ExtentHeapTest, create)
{
    TPtr<void> region = malloc(region_size);

    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
}

TEST(ExtentHeapTest, create_alloc_extent)
{
    TPtr<void> region = malloc(region_size);

    ExtentHeap_t* exheap = ExtentHeap_t::make(region, region_size, block_log2size);
    Extent ex;
    exheap->alloc_extent(1, &ex);
}



int main(int argc, char** argv)
{
    //::alps::init_test_env<::alps::TestEnvironment>(argc, argv);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
