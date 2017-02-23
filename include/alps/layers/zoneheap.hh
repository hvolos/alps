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

//#ifndef _ALPS_GLOBALHEAP_HH_
//#define _ALPS_GLOBALHEAP_HH_

#include <set>

#include "alps/globalheap/memattrib.hh"

#include "globalheap/lease.hh"

namespace alps {

// forward declaration
class Zone;

struct nvZoneHeader {
    // first cacheline
    uint32_t     magic;
    uint32_t     metazone_log2size;
    uint64_t     zone_size;
    Zone*        zone; // pointer to the zone's volatile descriptor for quick lookup
    //uint8_t      ig; // interleave group
    //uint8_t      reserved[23];
    // second cacheline
    //struct Lease lease; // must be cache aligned
};

//static_assert(sizeof(nvZoneHeader) == 128, "nvZoneHeader must be multiple of cache-line size");

template<template<typename> class TPtr>
struct nvZone {
    static TPtr<nvZone> make(TPtr<void> region, size_t zone_size, size_t metazone_log2size)
    {
        TPtr<nvZone> nvzone = region;
        // Set and persist header fields
        //nvzone->header.metazone_log2size = uint32_t(metazone_log2size);
        //nvzone->header.zone_size = zone_size;

        //Lease::make(&nvzone->header.lease);
        //persist((void*)&nvzone->header, sizeof(nvzone->header));

        return nvzone;
    }

#if 0
    static size_t zone_id(TPtr<nvHeap> nvheap, TPtr<nvZone> nvzone)
    {
        // operands are of different types so we cast them to a common type 
        // and do the pointer arithmetic ourselves 
        return (TPtr<char>(nvzone) - TPtr<char>(nvheap)) >> nvzone->header.metazone_log2size;
    }
#endif

    // first cacheline (header takes two cachelines)
    struct nvZoneHeader  header; 
    //char                 reserved[56];
    // fourth cacheline
    char                 payload[0];
};


/**
 * @brief A heap for allocating memory zones
 * 
 * @details
 * ZoneHeap is not thread safe. User has to serialize requests to a ZoneHeap
 * object. 
 * 
 * Zones acquired by a ZoneHeap are not released until process termination. 
 *
 */
template<template<typename> class TPtr>
class ZoneHeap {
protected:
    //typedef std::set<Zone*> ZoneSet;

public:
    ZoneHeap(TPtr<void> region, Generation generation)
        : generation_(generation),
          memattrib_(0)
    { }

    ZoneHeap(TPtr<void> region, Generation generation, const MemAttrib& memattrib)
        : generation_(generation),
          memattrib_(memattrib)
    { }

    ErrorStack init();
    ErrorStack teardown();

//    TPtr<nvZone<TPtr> > nvzone(TPtr<void> ptr);
//    TPtr<nvZone<TPtr> > nvzone(size_t zone_id);
//    MemAttrib nvzone_memattrib(size_t zone_id);
//    Zone* zone(size_t zone_id);

protected:
    Generation            generation_;
    MemAttrib             memattrib_;
    //ZoneSet               zones_;
};


} // namespace alps
