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

#ifndef ARRAY_BUFFER_POOL_H_
#define ARRAY_BUFFER_POOL_H_

#include <stdlib.h>
#include <tbb/concurrent_queue.h>
#include <iostream> 

using namespace tbb;
using namespace std; 

struct ArrayBufferElement {
  //the capacity of the current key/value buffer.
  size_t capacity; 
  //the start address for the current key/value buffer.
  unsigned char* start_address; 

  ArrayBufferElement():  
      capacity(0), start_address(nullptr) {
  }

  ArrayBufferElement (size_t c, unsigned char* sa):
                  capacity(c), start_address(sa) {
  }
  

};

//a general interface for a array-buffer pool to reuse the key or value buffers created to hold the value
//in the DRAM, and when the map or reduce finishes, the corresponding single key/value buffer should go to the pool.
class ArrayBufferPool {
 public: 

  //perform some initialization 
  virtual void initialize() =0 ; 
  //to free all of the key buffers held, then shutdown. 
  virtual void shutdown() = 0;

   //to ask the pool for a pooled key buffer that can be re-used
  virtual ArrayBufferElement getBuffer() =0 ; 
  virtual void freeBuffer(const ArrayBufferElement &) =0 ;

  virtual size_t currentSize() = 0;
  //for debugging purpose, to iterate through all of the byte buffer's pointer address.
  virtual void inspectBuffers() = 0; 

  virtual  ~ArrayBufferPool() {
    //do nothing
  }

};

//a null array-buffer pool, so that we can use it later to evaluate how effective the Arraybuffer pool
//is when switching between NullArrayBufferPool and QueueBasedArrayBufferPool.
class NullArrayBufferPool: public ArrayBufferPool {
 private: 
    //nothing  
 public:
  
  NullArrayBufferPool () {
    //do nothing
  }


  virtual ~NullArrayBufferPool() {
    //do nothing
  }   

 public:

  void initialize() override;

  //do nothing, as no array buffers get pooled.
  void shutdown() override;

  size_t currentSize() override {
     return 0;
  }

  //no array buffers to be pooled, so always return null pointer.
  ArrayBufferElement getBuffer() override {
    ArrayBufferElement result;
    return result;
  }

  //no array buffers to be pooled. so always free it.
  void freeBuffer(const ArrayBufferElement &element) override {
      free (element.start_address);
  }

  //for debugging purpose. here no array buffers exist.
  void inspectBuffers() override;

};

//the concrete implementation that is based on concurrent queue using Intel TBB. 
class QueueBasedArrayBufferPool: public ArrayBufferPool {
 private:
   //the queue that holds the pointer to array-buffers. 
   concurrent_queue <ArrayBufferElement> bufferPool;
   //the size of the queue, when the size gets excceeded, no one enqueing of
   //arraybuffer pool 
   size_t size;

 public: 
    
   //pass-in the size, which is the maximum number of elements that we will held
   //in the pool. beyond that, we will free it.
   QueueBasedArrayBufferPool(size_t dsize) {
      //do nothing. the queue start with zero byte buffer inside.
      size = dsize; 
   }

   virtual ~QueueBasedArrayBufferPool() {
     //we should already call freeAll before invoke the destructor.
   }

 public: 

   //nothing to be initialized
   void initialize() override;

   size_t currentSize() override {
     return bufferPool.unsafe_size();
   }

   //if the pool is not empty, return the front element(key buffer).
   //if the pool is empty, return empty/initialized key buffer.
   ArrayBufferElement getBuffer() {
       ArrayBufferElement result; //with empty initialization
       bufferPool.try_pop (result);
       //if the pool is empty, we will get capacity=0, start_address=nullptr.
       return result; 
   }

   //free the buffer, is to enqueue the buffer pointer. But if the current
   //number of buffers exceed the limits, simply free it.
   void freeBuffer(const ArrayBufferElement &element) override {
     if (bufferPool.unsafe_size() > size) {
         //do the direct freeing, without pooling the buffer specified in the input.
         free(element.start_address);
     }
     else{
          //enque it to the queue for next task thread
          bufferPool.push(element); 
     }
   }

    //when this get call, it is when the system is to be shut down and now task
    //threads should be using the pool.
   void shutdown() override;

   //for debugging purpose. here no byte buffers exist.
   void inspectBuffers() override;
};

#endif  /*ARRAY_BUFFER_POOL_H_*/
