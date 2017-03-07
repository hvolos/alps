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
#include "alps/layers/extentheap.hh"
#include "alps/layers/hybridheap.hh"

#include "test_common.hh"

using namespace alps;

class Context {

};

typedef SlabHeap<Context, TPtr, PPtr> SlabHeap_t;
typedef ExtentHeap<Context, TPtr, PPtr> ExtentHeap_t;
typedef HybridHeap<Context, TPtr, PPtr, SlabHeap_t, ExtentHeap_t> HybridHeap_t;

TEST(HybridHeapTest, alloc)
{
    Context ctx;
    SlabHeap_t slabheap;
} 


int main(int argc, char** argv)
{
    ::alps::init_test_env<::alps::TestEnvironment>(argc, argv);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
