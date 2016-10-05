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

#ifndef _ALPS_GLOBALHEAP_SLAB_H_
#define _ALPS_GLOBALHEAP_SLAB_H_

#include <pthread.h>
#include <stdint.h>
#include <atomic>
#include <list>
#include <iostream>

#include "common/debug.hh"
#include "globalheap/nvslab.hh"


namespace alps {

// forward declarations
class SlabHeap;
class Slab;

const int kSlabFullnessBins = 3;

typedef std::list<Slab*> SlabList;

/**
 * @brief Per-process private volatile Slab descriptor wrapping underlying 
 * shared non-volatile Slab
 *
 * @details
 * Slabs are owned and managed by a SlabHeap and any call for allocating/freeing 
 * blocks in a slab must be done through the SlabHeap that owns the slab.
 *
 */
class Slab
{
public:
    Slab(RRegion::TPtr<nvSlab> nvslab)
        : nvslab_(nvslab),
          slab_list_(NULL)
    { 
        init();
        nvslab->set_slab(this);
    }

    void init();

    void init(int sizeclass);

    int sizeclass() const 
    {
        return nvslab_->sizeclass();
    }

    size_t block_size() const
    {
        return nvslab_->block_size();
    }

    size_t block_offset(int index)
    {
        return nvslab_->block_offset(index);
    }

    bool full() 
    {
        return (nblocks_free() == 0);
    }

    bool empty()
    {
        return nblocks_free() == nblocks();
    }

    size_t nblocks() const
    {
        return nvslab_->nblocks();
    }

    size_t nblocks_free() const
    {
        return free_list_.size();
    }

    void set_owner(SlabHeap* owner)
    {
        owner_.store(owner, std::memory_order_seq_cst);
    }

    SlabHeap* owner() 
    {
        return owner_.load(std::memory_order_seq_cst);
    }
 
    inline void insert(SlabList* slab_list);
    inline void remove();

    int fullness();
    
    RRegion::TPtr<void> alloc_block();
    void free_block(RRegion::TPtr<void> ptr);

    void stream_to(std::ostream& os) const {
        os << "(" << block_size() << ", " << nblocks() << ", " << nblocks_free() << ")";
    }

    pthread_mutex_t        pin_mutex_;
    std::atomic<SlabHeap*> owner_;
    std::list<size_t>      free_list_;
    RRegion::TPtr<nvSlab>  nvslab_;
    SlabList*              slab_list_; // list this slab belongs to
    SlabList::iterator     slab_list_it_; // position in the slab list
};

inline std::ostream& operator<<(std::ostream& os, const Slab& slab)
{
    slab.stream_to(os);
    return os;
}

inline int Slab::fullness()
{
    return ((kSlabFullnessBins - 1) * (nblocks() - nblocks_free())) / nblocks();
}

void Slab::insert(SlabList* slab_list)
{
    assert(slab_list_ == NULL);
    slab_list->push_front(this);
    slab_list_it_ = slab_list->begin();
    slab_list_ = slab_list;
}

void Slab::remove()
{
    assert(slab_list_ != NULL);
    slab_list_->erase(slab_list_it_);
    slab_list_ = NULL;
}

} // namespace alps

#endif // _ALPS_GLOBALHEAP_SLAB_H_
