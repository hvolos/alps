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

#ifndef _ALPS_PEGASUS_REGION_HH_
#define _ALPS_PEGASUS_REGION_HH_

#include <vector>
#include "common/error_code.hh"
#include "pegasus/interleave_group.hh"
#include "pegasus/segmap.hh"

namespace alps {

// forward declarations
class AddressSpace;


/**
 * @brief A template class for defining region types.
 * 
 * We expect most region types to differ in the way they map the underlying 
 * region file(s). Therefore, we provide a template class for defining
 * region types where the mapping policy/behavior is defined through a 
 * MemoryMap type. 
 *
 */
template<class MemoryMapT>
class RegionTmpl: public Region {
friend class AddressSpace;
    using MemoryMap = MemoryMapT;

public:
    RegionTmpl(AddressSpace* address_space, RegionFile* file);

    ErrorCode map();
    ErrorCode unmap();

#if 0
    ErrorCode resize(const loff_t length, loff_t* new_length);
#endif

    ErrorCode set_interleave_group(loff_t offset, loff_t length, const std::vector<InterleaveGroup>& vig);
    ErrorCode interleave_group(loff_t offset, loff_t length, std::vector<InterleaveGroup>* vig);

    template<class T>
    using TPtr = TPtr<RegionTmpl,T>;

    template<class T>
    using PPtr = PPtr<RegionTmpl,T>;

    template<class T>
    using ZPtr = ZPtr<RegionTmpl,T>;

    template<class T>
    TPtr<T> base(LinearAddr offset)
    {
        //return TPtr<T>(this, offset);
        void* base_ptr = reinterpret_cast<void*>(mem_map_->trans(offset));
        return TPtr<T>(base_ptr);
    }

    MemoryMap* mem_map() 
    {
        return mem_map_;
    }

    uintptr_t trans(LinearAddr offset)
    {
        return mem_map_->trans(offset);
    }

private:
    MemoryMap*  mem_map_; // address space memory map 
};

using RRegion = RegionTmpl<SegmentMap>;

} // namespace alps

#endif // _ALPS_PEGASUS_REGION_HH_
