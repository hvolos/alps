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

#include "pegasus/address_space.hh"
#include "common/error_code.hh"
#include "common/log.hh"
#include "pegasus/address_space_options.hh"
#include "pegasus/lfs_region_file.hh"
#include "pegasus/mm.hh"
#include "pegasus/region_file.hh"
#include "pegasus/region_tmpl.hh"
#include "pegasus/pegasus_options.hh"
#include "pegasus/tmpfs_region_file.hh"


namespace alps {


AddressSpace* __pegas; // pegas singleton instance

AddressSpace::AddressSpace(AddressSpaceOptions& address_space_options)
    : address_space_options_(address_space_options)
{ }


ErrorCode AddressSpace::init()
{
    if (!(mm_ = new MemoryManager())) {
        return kErrorCodeOutofmemory;
    }

    return kErrorCodeOk;   
}

template<class RegionT>
ErrorCode AddressSpace::map(RegionFile* region_file, RegionT** pregion)
{
    RegionT* region;

    if (!(region = new RegionT(this, region_file))) {
        return kErrorCodeOutofmemory;
    }
    if (address_space_options_.allow_duplicate_mappings == false && 
        regions_.find(region->id()) != regions_.end()) 
    {
        delete region;
        return kErrorCodeMemoryDuplicateMapping;
    }

    CHECK_ERROR_CODE(region->map());

    *pregion = region;
    regions_.insert(std::pair<RegionId, Region*>(region->id(), region));
    return kErrorCodeOk;
}


template<class RegionT>
ErrorCode AddressSpace::unmap(RegionT* region)
{
    std::multimap<RegionId, Region*>::iterator it = regions_.find(region->id());
    if (it == regions_.end()) {
        return kErrorCodeMemoryUnknownMapping;
    }
    CHECK_ERROR_CODE(region->unmap());
    regions_.erase(it);
    return kErrorCodeOk;
}


Region* AddressSpace::region(RegionId region_id)
{
    std::multimap<RegionId, Region*>::iterator it = regions_.find(region_id);
    if (it == regions_.end()) {
        return NULL;
    }
    return it->second;
}

// explicit instantiations
template ErrorCode AddressSpace::map<RRegion>(RegionFile* region_file, RRegion** pregion);
template ErrorCode AddressSpace::unmap<RRegion>(RRegion* region);


} // namespace alps
