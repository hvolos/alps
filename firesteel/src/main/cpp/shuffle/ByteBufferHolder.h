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
 *
 */

#ifndef BYTEBUFFERHOLDER_H_
#define BYTEBUFFERHOLDER_H_

#include <glog/logging.h>
#include "ByteBufferPool.h"
#include "ShuffleStoreManager.h"

#include <stdlib.h>
#include <vector>

using namespace std;

class ByteBufferHolder {
 private:
    size_t tcapacity; 
    //the cursor for the data to be inserted at this time.
    unsigned char *holder;
    //current position, as the holder is advancing.
    unsigned char *position;
    //remaining capacity at this time
    size_t rem_capacity; 

    //indicating whether this is self-created.
    bool self_created; 

 public: 
    
    //initialize the buffer with the specified capacity.
    ByteBufferHolder (size_t cap): tcapacity(cap) {
       unsigned char* avail_buf =
	 ShuffleStoreManager::getInstance()->getByteBufferPool()->getBuffer();
       if (avail_buf != nullptr) {
 	  holder = avail_buf;  //get from the pool
       }
       else{
          holder =(unsigned char*) malloc (tcapacity);
       }

       position = holder;
       rem_capacity = tcapacity;
       self_created=true;
    }

    //initialize the buffer from an existing byte buffer, such as 
    //the one from Java-side's ByteBuffer.

    ByteBufferHolder (unsigned char *h, size_t cap): 
        tcapacity(cap),  holder(h), 
	position(h),  rem_capacity (cap),
        self_created(false)  {
	  //pointer is passed in, thus it is not self created.
    }

    //Item 7: declare a virtual destructor if and only if the class contains at least one virtual function 
    ~ByteBufferHolder() {
    }

    size_t capacity() const {
       return tcapacity;
    }

    size_t remain_capacity() const{
      return rem_capacity; 
    }
    
    int position_in_buffer() const {
      return (position - holder); 

    }

    unsigned char *position_at (int position) const {
      return holder + position; 
    }

    unsigned char value_at(int i) const {
      return *(holder+i);
    }

    void free_buffer() {
      //only free the buffer when it is self-created.
      if (self_created) {
        //to have the one (either created, or get from the pool), return to the pool.
        ShuffleStoreManager::getInstance()->getByteBufferPool()->freeBuffer(holder);
        //free(holder);
        holder=nullptr;
        position=0;
        rem_capacity=0;
      }
    }

    void reset() {
       static int counter=0; 
       VLOG (3) << "holder counter reset with " << counter << " times" ;
       counter++;

       position = holder;
       rem_capacity = tcapacity;
    }

    //the ByteBuffer manager will make sure that this append operation
    //only span across a single buffer
    //void * memcpy ( void * destination, const void * source, size_t num );
    unsigned char* append_inbuffer(unsigned char* buffer, size_t size) {
            unsigned char *curPosition=position;
            memcpy (position, buffer, size);
            position +=size;
            rem_capacity -=size; 
            return curPosition;
    }
};

#endif 
