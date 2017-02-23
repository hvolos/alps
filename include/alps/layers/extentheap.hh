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


namespace alps {

#define kCacheLineSize 64

struct nvBlock {
    uint8_t data[0];
};


template<template<typename> class TPtr>
struct nvExtentHeader {
public:
    enum {
        kBlockTypeFree = 0,
        kBlockTypeExtentFirst = 1, // first block of an extent
        kBlockTypeExtentRun = 2,   // run block of an extent 
    };

    static TPtr<nvExtentHeader> make(TPtr<nvExtentHeader> header, uint8_t type)
    {
        header->type_ = type;
        //persist((void*)&header, sizeof(nvExtentHeader));
        return header;
    }

    bool is_free() {
        return (type_ == kBlockTypeFree);
    }

    void mark_alloc(uint32_t nblocks)
    {
        size_ = nblocks;
        nvExtentHeader* this_bh = reinterpret_cast<nvExtentHeader*>(this);
        for (uint32_t i=1; i<nblocks; i++) {
            nvExtentHeader* bh = this_bh + i;
            bh->type_ = nvExtentHeader::kBlockTypeExtentRun;
            //persist((void*) &bh->type_, sizeof(bh->type_));
        }

        // Linearization point (with respect to failures)
        // 
        // Persisting the primary_type of the first block is a single atomic 
        // step that identifies the block group as an extent run.
        this_bh->type_ = nvExtentHeader::kBlockTypeExtentFirst;
        //persist((void*) &this_bh->primary_type, sizeof(this_bh->primary_type));
    }

    void mark_free()
    {
        uint32_t nblocks = size_;
        nvExtentHeader* this_bh = reinterpret_cast<nvExtentHeader*>(this);
        for (uint32_t i=0; i<nblocks; i++) {
            nvExtentHeader* bh = this_bh + i;
            bh->type_ = nvExtentHeader::kBlockTypeFree;
        }
    }

//protected:
    /** Block type */
    uint8_t  type_;

    /** Extent size in number of blocks */
    uint32_t size_;
};

template<template<typename> class TPtr, template<typename> class PPtr>
class ExtentHeap;

struct nvExtentHeapHeader {
    union {
        struct {
            // first cacheline
            uint32_t  magic;
            uint64_t  region_size;
            uint64_t  block_log2size_;
            uint64_t  nblocks;
            uint64_t  extent_headers_offset_; // extent headers offset relative to payload
            uint64_t  blocks_offset_; // blocks offset relative to payload
            void*     extentheap_; // pointer to the heap's volatile descriptor for quick lookup
        };
        uint8_t u8_[64];
    };
};

static_assert(sizeof(nvExtentHeapHeader) == 64, "nvExtentHeapHeader must be multiple of cache-line size");

template<template<typename> class TPtr, template<typename> class PPtr>
struct nvExtentHeap {
public:
    static TPtr<nvExtentHeap> make(TPtr<void> region, size_t region_size, size_t block_log2size)
    {
        // Calculate number of blocks that can fit in the zone and set 
        // the block_header and block pointers accordingly.
        // The first block must be aligned at cache-line multiple so that it
        // doesn't share a cacheline with the last block-header. 
        size_t block_size = 1LLU << block_log2size;
        size_t effective_region_size = region_size - sizeof(nvExtentHeap);
        size_t max_nblocks = effective_region_size / (sizeof(nvExtentHeader<TPtr>) + block_size); 
        // Adjust (reduce) number of blocks to accomodate extra space needed
        // for alignment.
        size_t extent_headers_aligned_total_size = round_up(max_nblocks * sizeof(nvExtentHeader<TPtr>), kCacheLineSize);
        size_t blocks_total_size = effective_region_size - extent_headers_aligned_total_size;
        size_t nblocks = blocks_total_size / block_size;

        assert((sizeof(nvExtentHeap<TPtr,PPtr>) + (sizeof(nvExtentHeader<TPtr>) + block_size) * nblocks <= region_size)); 

        // Set and persist header fields
        TPtr<nvExtentHeap<TPtr, PPtr> > exheap = region;
        exheap->header_.region_size = region_size;
        exheap->header_.block_log2size_ = block_log2size;
        exheap->header_.nblocks = nblocks;
        //exheap->extent_headers = &exheap->payload[0];
        //exheap->blocks = static_cast<TPtr<nvBlock>>((nvBlock*)&exheap->payload[extent_headers_aligned_total_size]);
        exheap->header_.extent_headers_offset_ = 0;
        exheap->header_.blocks_offset_ = extent_headers_aligned_total_size;
        //persist((void*)&exheap->header, sizeof(exheap->header));

        // Format block headers
        for (size_t i=0; i<exheap->header_.nblocks; i++) {
            nvExtentHeader<TPtr>::make(exheap->extent_header(i), nvExtentHeader<TPtr>::kBlockTypeFree);
        } 
        return exheap;
    }

    static TPtr<nvExtentHeap> load(TPtr<void> region)
    {
        return reinterpret_cast<nvExtentHeap<TPtr, PPtr>*>(region.get());
    }

    TPtr<nvExtentHeader<TPtr> > extent_header(int idx)
    {
        return &payload_[header_.extent_headers_offset_ + idx*sizeof(nvExtentHeader<TPtr>)];
    }
 
    TPtr<nvBlock> block(int idx) 
    {
        size_t block_size = 1LLU << header_.block_log2size_;
        return &payload_[header_.blocks_offset_ + idx * block_size];
    }

    ExtentHeap<TPtr, PPtr>* extentheap() 
    {
        return reinterpret_cast<ExtentHeap<TPtr, PPtr>*>(header_.extentheap_); // pointer to the heap's volatile descriptor for quick lookup
    }

    void set_extentheap(ExtentHeap<TPtr, PPtr>* extentheap) {
        header_.extentheap_ = reinterpret_cast<void*>(header_.extentheap_);
    }

private:
    size_t find_extent(size_t begin, size_t end, ExtentInterval* extent, bool* extent_is_free) 
    {
        size_t i;
        size_t ext_begin = begin;
        size_t ext_end = begin;

        *extent_is_free = false;
        for (i=begin; i<end; i++) {
            TPtr<nvExtentHeader<TPtr> > exh = extent_header(i);
            if (exh->type_ == nvExtentHeader<TPtr>::kBlockTypeFree) {
                *extent_is_free = true;
                ext_end = i + 1;
            } else if (exh->type_ == nvExtentHeader<TPtr>::kBlockTypeExtentFirst) {
                if (ext_end > ext_begin) {
                    // report the free extent we found before this one
                    break;
                } else {
                    ext_begin = i;
                    ext_end = ext_begin + exh->size_;
                    break;
                }
            }
        }

        size_t ext_len = ext_end - ext_begin;
        *extent = ExtentInterval(ext_begin, ext_len);

        return ext_begin;
    }

public:
    class Iterator {
    public:
        Iterator()
        { }

        Iterator(TPtr<nvExtentHeap<TPtr, PPtr>> nvexheap, size_t begin, size_t end, size_t cur)
            : nvexheap_(nvexheap),
              begin_(begin),
              end_(end),
              cur_(cur),
              next_cur_(cur)
        {
            next();
        }

        Iterator& operator++()
        {
            next();
            return *this;
        }  

        bool operator==(const Iterator& other) 
        {
            return begin_ == other.begin_ && end_ == other.end_ && cur_ == other.cur_;
        }

        bool operator!=(const Iterator& other) 
        {
            return !(*this == other);
        }

        ExtentInterval operator*()
        {
            return cur_extent_;
        }

        void next()
        {
            bool extent_is_free;
            cur_ = nvexheap_->find_extent(next_cur_, end_, &cur_extent_, &extent_is_free); 
            if (cur_ < end_) {
                next_cur_ = cur_extent_.start() + cur_extent_.len();
            }
        }

        void stream_to(std::ostream& os) const {
            os << "(" << begin_ << ", " << end_ << ", " << cur_ << ")";
        }

        std::string to_string() const {
            std::stringstream ss;
            stream_to(ss);
            return ss.str();
        }

    //private:
        TPtr<nvExtentHeap<TPtr, PPtr>> nvexheap_;
        size_t                         begin_;
        size_t                         end_;
        size_t                         cur_;
        size_t                         next_cur_;
        ExtentInterval                 cur_extent_;
    };

    Iterator begin() 
    {
        return Iterator(this, 0, header_.nblocks, 0);
    }

    Iterator end()
    {
        return Iterator(this, 0, header_.nblocks, header_.nblocks);
    }

//private:
    struct nvExtentHeapHeader header_; 
    uint8_t                   payload_[0];
};

template<template<typename> class TPtr, template<typename> class PPtr>
class ExtentHeap;


template<template<typename> class TPtr, template<typename> class PPtr>
class ExtentDesc {
public:


ExtentHeap<TPtr,PPtr>* exheap_;

};


/**
 * @brief Manages a heap of extents
 */
template<template<typename> class TPtr, template<typename> class PPtr>
class ExtentHeap {
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

#if 0
    bool has_free_space(size_t size_nblocks)
    {
        return fsmap_.exists_extent(size_nblocks);
    }
#endif

    //TODO: decouple volatile from persistent allocation
    //FIXME: return Extent as follows:
    ErrorCode alloc_extent(size_t size_nblocks, ExtentInterval* extent)
    //ErrorCode alloc_extent(size_t size_nblocks, TPtr<nvExtentHeader<TPtr> >* nvexheader, TPtr<void>* nvex)
    {
        ExtentDesc<TPtr, PPtr> exd;
        Extent ex;
        /*
        if (fsmap_.alloc_extent(size_nblocks, &ex) == 0) {
            *nvexheader = static_cast<TPtr<nvExtentHeader<TPtr>>>(nvexheap_->block_header(ex.start()));
            (*nvexheader)->mark_alloc(ex.len());
            TPtr<nvBlock> nvblock = nvexheap_->block(ex.start());
            *nvex = TPtr<void>(nvblock);
            //LOG(info) << "Allocated extent: " << ex << " " << (*nvex).get();
            return kErrorCodeOk;
        }
        */
        return kErrorCodeOutofmemory;
    }

    //TODO: decouple volatile from persistent allocation
    //TODO: implement Extent based interface
    void free_extent(const Extent& extent)
    {

    }

    void free_extent(TPtr<void> ptr)
    {
        TPtr<nvBlock> nvblock_base = nvexheap_->block(0);
        uintptr_t offset = ptr.offset() - nvblock_base.offset();
        size_t idx = offset >> nvexheap_.header_.block_log2size_;
        TPtr<nvExtentHeader<TPtr>> nvexheader = static_cast<TPtr<nvExtentHeader<TPtr>>>(nvexheap_->block_header(idx));
        fsmap_.free_extent(Extent(idx, nvexheader->size));
        nvexheader->mark_free();
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

private:

#if 0
    ErrorStack init()
    {
        Extent extent;
        bool   extent_is_free;
        size_t nblocks = nvzone_->nblocks();

        for (size_t next_start = 0;
             find_extent(nvzone_, next_start, nblocks, &extent, &extent_is_free) < nblocks; ) 
        {
            next_start = extent.start() + extent.len();
            if (extent_is_free) {
                insert(extent);
            }
        }
        return kRetOk;
    }
#endif

    ErrorStack init()
    {
        typename nvExtentHeap<TPtr, PPtr>::Iterator it;
        std::cout << "INIT" << std::endl;
        for (it = nvexheap_->begin(); it != nvexheap_->end(); ++it) {
            std::cout << *it << std::endl;       
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
    /**
     * @brief Allocate an extent from zones already owned by this extent heap.
     * 
     * @param[in] size_nblocks size of extent in number of blocks
     * @param[in] zone_hint hint to a zone that might have free space
     * @param[out] nvex a pointer to the extent 
     * @param[out] zone the zone we allocated extent from
     */
//    ErrorCode alloc_extent(size_t size_nblocks, Zone* zone_hint, RRegion::TPtr<nvExtentHeader>* nvexheader, RRegion::TPtr<void>* nvex, Zone** pzone);

private:
    pthread_mutex_t                mutex_;
    TPtr<nvExtentHeap<TPtr, PPtr>> nvexheap_;
    FreeSpaceMap<TPtr>             fsmap_;        
};


} // namespace alps

#endif // _ALPS_LAYERS_EXTENTHEAP_HH_
