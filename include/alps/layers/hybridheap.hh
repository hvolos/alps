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

#ifndef _ALPS_LAYER_HYBRIDHEAP_HH_
#define _ALPS_LAYER_HYBRIDHEAP_HH_

#include "alps/common/assert_nd.hh"

namespace alps {

/**
 * @brief Hybrid heap 
 *
 * @details 
 * 
 */
template<typename Context, template<typename> class TPtr, template<typename> class PPtr, typename SmallHeap, typename BigHeap>
class HybridHeap
{
public:

    ErrorCode malloc(Context& ctx, size_t size_bytes, TPtr<void>* ptr)
    {

    }

    void free(Context& ctx, TPtr<void> ptr) 
    {

    }
 
protected:
    SmallHeap sh_;
    BigHeap bh_;
};

} // namespace alps


#endif _ALPS_LAYER_HYBRIDHEAP_HH_
