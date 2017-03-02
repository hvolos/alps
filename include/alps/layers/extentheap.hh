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

#ifndef _ALPS_LAYERS_EXTENTHEAP_HH_
#define _ALPS_LAYERS_EXTENTHEAP_HH_

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <map>

#include "alps/common/assorted_func.hh"
#include "alps/common/error_code.hh"
#include "alps/common/error_stack.hh"
#include "alps/globalheap/memattrib.hh"
#include "alps/layers/bits/extentinterval.hh"
#include "alps/layers/bits/freespacemap.hh"
#include "alps/layers/bits/nvextentheap.hh"


namespace alps {

template<template<typename> class TPtr, template<typename> class PPtr>
class ExtentHeap;


template<template<typename> class TPtr, template<typename> class PPtr>
class Extent {
    friend ExtentHeap<TPtr,PPtr>;
public:
    Extent()
        : exheap_(NULL)
    { }

    Extent(ExtentHeap<TPtr,PPtr>* exheap, size_t start, size_t len)
        : exheap_(exheap),
          interval_(start, len)
    { 
        init();
    }

    ExtentInterval interval()
    {
        return interval_;
    }

    TPtr<nvExtentHeader<TPtr>> nvheader()
    {
        return nvextentheader_;
    }

    TPtr<void> nvextent()
    {
        return nvextent_;
    }

    bool operator==(const Extent<TPtr, PPtr> &other) const 
    {  
        return interval_ == other.interval_;  
    }

    void stream_to(std::ostream& os) const 
    {
        os << interval_;
    }

private:
    void init()
    {
        TPtr<nvExtentHeap<TPtr, PPtr>> nvexheap = exheap_->nvexheap_;
        nvextentheader_ = static_cast<TPtr<nvExtentHeader<TPtr>>>(nvexheap->extent_header(interval_.start()));
        nvextent_ = nvexheap->block(interval_.start());
    }

    void mark_alloc()
    {
        nvextentheader_->mark_alloc(interval_.len());
    }

    void mark_free()
    {
        nvextentheader_->mark_free();
    }
    
    ExtentHeap<TPtr,PPtr>*     exheap_;
    ExtentInterval             interval_;
    TPtr<nvExtentHeader<TPtr>> nvextentheader_;
    TPtr<void>                 nvextent_;
};

inline std::ostream& operator<<(std::ostream& os, const Extent<TPtr,PPtr>& ex)
{
    ex.stream_to(os);
    return os;
}

//template<template<typename> class TPtr, template<typename> class PPtr>


/**
 * @brief Manages a heap of extents
 */
template<template<typename> class TPtr, template<typename> class PPtr>
class ExtentHeap {
    friend Extent<TPtr,PPtr>;

public:
    static ExtentHeap* make(TPtr<void> region, size_t region_size, size_t block_log2size)
    {
        ExtentHeap* exheap = new ExtentHeap;

        exheap->nvexheap_ = nvExtentHeap<TPtr, PPtr>::make(region, region_size, block_log2size);
        exheap->init();

        return exheap;
    }

    static ExtentHeap* load(TPtr<void> region)
    {
        ExtentHeap* exheap = new ExtentHeap;

        exheap->nvexheap_ = nvExtentHeap<TPtr, PPtr>::load(region);
        exheap->init();

        return exheap;
    }

    uint64_t blocksize()
    {
        return 1 << nvexheap_->header_.block_log2size_;
    }

#if 0
    bool has_free_space(size_t size_nblocks)
    {
        return fsmap_.exists_extent(size_nblocks);
    }
#endif

    ErrorCode extent(ExtentInterval interval, Extent<TPtr,PPtr>* ex)
    {
        *ex = Extent<TPtr,PPtr>(this, interval.start(), interval.len());
        return kErrorCodeOk;
    }

    ErrorCode extent(TPtr<void> ptr, Extent<TPtr,PPtr>* ex)
    {
        TPtr<nvBlock> nvblock = ptr;

        if (nvblock < nvexheap_->block(0) || 
            nvblock >= nvexheap_->block(0) + nvexheap_->header_.region_size_)
        {
            return kErrorCodeMemoryInvalidAddress;
        }
        uintptr_t diff = nvblock - nvexheap_->block(0);
        size_t idx = diff >> nvexheap_->header_.block_log2size_;
        TPtr<nvExtentHeader<TPtr>> exhdr = nvexheap_->extent_header(idx);
        *ex = Extent<TPtr,PPtr>(this, idx, exhdr->size());

        return kErrorCodeOk;
    }

    ErrorCode alloc_extent(size_t size_nblocks, Extent<TPtr,PPtr>* ex)
    {
        ExtentInterval exintv;
        if (fsmap_.alloc_extent(size_nblocks, &exintv) == 0) {
            *ex = Extent<TPtr,PPtr>(this, exintv.start(), exintv.len());
            ex->mark_alloc();
            LOG(info) << "Allocated extent: " << ex;
            return kErrorCodeOk;
        }
        return kErrorCodeOutofmemory;
    }

    ErrorCode free_extent(Extent<TPtr,PPtr>& ex)
    {
        fsmap_.free_extent(ex.interval());
        ex.mark_free();
        return kErrorCodeOk;
    }

    ErrorCode free_extent(TPtr<void> ptr)
    {
        Extent<TPtr,PPtr> ex;
        CHECK_ERROR_CODE(extent(ptr, &ex));
        return free_extent(ex);
    }

    //acquire(TPtr<void> ptr)

    ErrorCode malloc(size_t size_bytes, TPtr<void>* nvex)
    {
#if 0
        ErrorCode rc;
        Zone*     zone;

        LOG(info) << "size_bytes == " << size_bytes;

        pthread_mutex_lock(&mutex_);

        // round up to next multiple of block_size
        size_t size_nblocks = size_bytes / nvZone::block_size() + (size_bytes % nvZone::block_size() ? 1: 0);

        rc = alloc_extent(size_nblocks, nvexheader, nvex);
        pthread_mutex_unlock(&mutex_);
        return rc;
#endif
    }

    void free(TPtr<void> nvex)
    {
#if 0
        pthread_mutex_lock(&mutex_);

        // find the extent's zone
        size_t metazone_size = nvheap_->metazone_size();
        size_t zone_id = (nvex.offset() & ~(metazone_size - 1)) / metazone_size;
        Zone* zone = acquire_zone(zone_id, callback);
        if (zone) {
            zone->free_extent(nvex);
        } else {
            LOG(error) << "Attempt to free unknown address: " << nvex << std::endl;
        }
        pthread_mutex_unlock(&mutex_);
#endif
    }

public:
    class Iterator {
    public:
        Iterator()
        { }

        Iterator(ExtentHeap<TPtr, PPtr>* exheap, typename nvExtentHeap<TPtr, PPtr>::Iterator nvit)
            : exheap_(exheap),
                //nvexheap_(exheap->nvexheap_),
              nvit_(nvit)
        { }

        Iterator& operator++()
        {
            ++nvit_;
            return *this;
        }  

        Extent<TPtr,PPtr> operator*()
        {
            ExtentInterval interval = *nvit_;
            return Extent<TPtr,PPtr>(exheap_, interval.start(), interval.len());
        }

        bool operator==(const Iterator& other) 
        {
            return nvit_ == other.nvit_;
        }

        bool operator!=(const Iterator& other) 
        {
            return nvit_ != other.nvit_;
        }

        ExtentHeap<TPtr, PPtr>* exheap_;
        typename nvExtentHeap<TPtr,PPtr>::Iterator nvit_;
    };

    Iterator begin()
    {
        return Iterator(this, nvexheap_->begin());
    }

    Iterator end()
    {
        return Iterator(this, nvexheap_->end());
    }

private:

    ErrorStack init()
    {
        typename nvExtentHeap<TPtr, PPtr>::Iterator it;
        for (it = nvexheap_->begin(); it != nvexheap_->end(); ++it) {
            if (nvexheap_->is_free(*it)) {
                LOG(info) << "Load free extent: " << *it;
                fsmap_.insert(*it);
            }
        }
        return kRetOk;
    }

#if 0
    template<typename T> ErrorCode malloc(size_t size, bool can_extend, T enumerate_callback, RRegion::TPtr<nvExtentHeader>* nvexheader, RRegion::TPtr<void>* nvex);
    RRegion::TPtr<void> malloc(size_t size);
    void free(Zone* zone, RRegion::TPtr<void> nvex);
    void free(RRegion::TPtr<void> nvex);
    template<typename T> void free(RRegion::TPtr<void> nvex, T enumerate_callback);

    template<typename T> Zone* acquire_zone(size_t zone_id, T enumerate_callback_functor);

    /**
     * @brief Acquire nzones zones and invoke functor callback for each zone acquired.
     * 
     */ 
    template<typename T> int more_space(int nzones, T callback);
#endif
    
private:
    pthread_mutex_t                mutex_;
    TPtr<nvExtentHeap<TPtr, PPtr>> nvexheap_;
    FreeSpaceMap<TPtr>             fsmap_;        
};


} // namespace alps

#endif // _ALPS_LAYERS_EXTENTHEAP_HH_
