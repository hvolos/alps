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

#ifndef BYTEBUFFER_POOL_H_
#define BYTEBUFFER_POOL_H_

#include <stdlib.h>
#include <tbb/concurrent_queue.h>
#include <iostream> 

using namespace tbb;
using namespace std; 

//a general interface for a bytebuffer pool to reuse the byte buffers created to hold the value
//in the DRAM, and when the map finishes, all the byte buffers should go to the pool.
class ByteBufferPool {
 public: 

  //perform some initialization 
  virtual void initialize() =0 ; 
  //to free all of the byte buffers held, then shutdown. 
  virtual void shutdown() = 0;

   //to ask the pool for a pooled byte buffer that can be re-used
  virtual unsigned char * getBuffer() =0 ; 
  virtual void freeBuffer(unsigned char * buffer) =0 ;

  virtual size_t currentSize() = 0;
  //for debugging purpose, to iterate through all of the byte buffer's pointer address.
  virtual void inspectBuffers() = 0; 

  virtual  ~ByteBufferPool() {
    //do nothing
  }

};

//a null bytebuffer pool, so that we can use it later to evaluate how effective the bytebuffer pool
//is when switching between NullByteBufferPool and QueueBasedByteBufferPool.
class NullByteBufferPool: public ByteBufferPool {
 private: 
    //nothing  
 public:
  
  NullByteBufferPool () {
    //do nothing
  }


  virtual ~NullByteBufferPool() {
    //do nothing
  }   

 public:

  void initialize() override;

  //do nothing, as no byte buffers get pooled.
  void shutdown() override;

  size_t currentSize() override {
     return 0;
  }

  //no byte buffers to be pooled, so always return null pointer.
  unsigned char * getBuffer() override {
     return nullptr; 
  }

  //no byte buffers to be pooled. so always free it.
  void freeBuffer(unsigned char * buffer) override {
      free (buffer);
  }

  //for debugging purpose. here no byte buffers exist.
  void inspectBuffers() override;

};

//the concrete implementation that is based on concurrent queue using Intel TBB. 
class QueueBasedByteBufferPool: public ByteBufferPool {
 private:
   //the queue that holds the pointer to bytebuffers. 
   concurrent_queue <unsigned char*> bufferPool;   
   //the size of the queue, when the size gets excceeded, no one enqueing of
   //bytebuffer pool 
   size_t size;

 public: 
    
   //pass-in the size
   QueueBasedByteBufferPool(size_t dsize) {
      //do nothing. the queue start with zero byte buffer inside.
      size = dsize; 
   }

   virtual ~QueueBasedByteBufferPool() {
     //we should already call freeAll before invoke the destructor.
   }

 public: 

   //nothing to be initialized
   void initialize() override;

   size_t currentSize() override {
       return bufferPool.unsafe_size();
   }

   //if the pool is not empty, return the pointer to the buffer.
   //if the pool is empty, return null pointer.
   unsigned char * getBuffer() {
       unsigned char *result = nullptr;
       //If value is available, pops it from the queue, assigns it to destination,
       //and destroys the original value. Otherwise does nothing
       bufferPool.try_pop (result);
       return result; 
   }

   //free the buffer, is to enqueue the buffer pointer. But if the current
   //number of buffers exceed the limits, simply free it.
   void  freeBuffer(unsigned char * buffer) {
     if (bufferPool.unsafe_size() > size) {
       //do the direct freeing, without pooling the buffer specified in the input.
       free(buffer);
     }
     else{
       //enque it to the queue for next task thread
       bufferPool.push(buffer);
     }

   }

    //when this get call, it is when the system is to be shut down and now task
    //threads should be using the pool.
   void shutdown() override;

   //for debugging purpose. here no byte buffers exist.
   void inspectBuffers() override;
};

#endif  /*BYTEBUFFER_POOL_H_*/
