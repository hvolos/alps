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

#ifndef _ALPS_GLOBALHEAP_MULTIPLE_EXTENTHEAP_HH_
#define _ALPS_GLOBALHEAP_MULTIPLE_EXTENTHEAP_HH_

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <map>

#include "common/error_code.hh"
#include "globalheap/layout.hh"
#include "globalheap/extent.hh"
#include "globalheap/extentheap.hh"
#include "globalheap/memattrib.hh"
#include "globalheap/memattrib_alloc.hh"
#include "globalheap/zone.hh"
#include "globalheap/zoneheap.hh"

namespace alps {

//forward declarations
class FreeSpaceMap;

/**
 * @brief Unites multiple extentheaps of different memory attributes under
 * a common interface.
 */
class MultiExtentHeap {
public:
    MultiExtentHeap(RRegion::TPtr<nvHeap> nvheap);

    template<typename T> ErrorCode malloc(size_t size, bool can_extend, T callback, const MemAttrib& memattrib, RRegion::TPtr<nvExtentHeader>* nvexheader, RRegion::TPtr<void>* nvex);
    void free(Zone* zone, RRegion::TPtr<void> nvex);
    void free(RRegion::TPtr<void> nvex);
    template<typename T> void free(RRegion::TPtr<void> nvex, T callback);

    template<typename T> Zone* acquire_zone(size_t zone_id, T enumerate_callback_functor);

    /**
     * @brief Acquire @a nzones zones and invoke functor callback for each 
     * zone acquired.
     * 
     */ 
    template<typename T> int more_space(int nzones, T callback, const MemAttrib& memattrib);

private:
    ExtentHeap* find_or_bind(const MemAttrib& memattrib);

private:
    pthread_mutex_t                 mutex_;
    RRegion::TPtr<nvHeap>           nvheap_;
    MemAttribAllocators<ExtentHeap> extentheaps_;
};

template<typename T>
int MultiExtentHeap::more_space(int nzones, T enumerate_callback_functor, const MemAttrib& memattrib)
{
    ExtentHeap* extentheap = find_or_bind(memattrib);
    return extentheap->more_space(nzones, enumerate_callback_functor);
}

template<typename T> 
ErrorCode MultiExtentHeap::malloc(size_t size, bool can_extend, T callback, const MemAttrib& memattrib, RRegion::TPtr<nvExtentHeader>* nvexheader, RRegion::TPtr<void>* nvex)
{
    ExtentHeap* extentheap = find_or_bind(memattrib);
    return extentheap->malloc(size, can_extend, callback, nvexheader, nvex);
}


} // namespace alps

#endif // _ALPS_GLOBALHEAP_MULTIPLE_EXTENTHEAP_HH_
