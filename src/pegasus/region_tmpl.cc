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

#include "pegasus/region_tmpl.hh"
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "common/debug.hh"
#include "common/error_code.hh"
#include "pegasus/region_file.hh"
#include "pegasus/region_file_factory.hh"

namespace alps {

template<class MemoryMapT>
RegionTmpl<MemoryMapT>::RegionTmpl(AddressSpace* address_space, RegionFile* region_file)
    : Region(address_space, region_file)
{ }


template<class MemoryMapT>
ErrorCode RegionTmpl<MemoryMapT>::map()
{
    CHECK_ERROR_CODE(file_->size(&length_));

    if (!(mem_map_ = new MemoryMapT(this))) {
        return kErrorCodeOutofmemory;
    }
 
    CHECK_ERROR_CODE(mem_map_->map_all());
    
    return kErrorCodeOk; 
}


template<class MemoryMapT>
ErrorCode RegionTmpl<MemoryMapT>::unmap()
{
    CHECK_ERROR_CODE(mem_map_->unmap_all());

    return kErrorCodeOk; 
}

template<class MemoryMapT>
ErrorCode RegionTmpl<MemoryMapT>::set_interleave_group(loff_t offset, loff_t length, const std::vector<InterleaveGroup>& vig)
{
    return file_->set_interleave_group(offset, length, vig);
}

template<class MemoryMapT>
ErrorCode RegionTmpl<MemoryMapT>::interleave_group(loff_t offset, loff_t length, std::vector<InterleaveGroup>* vig)
{
    return file_->interleave_group(offset, length, vig);
}

// explicit instantiations
template class RegionTmpl<SegmentMap>;


} // namespace alps
