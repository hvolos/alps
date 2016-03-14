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

#include <glog/logging.h>
#include "ByteBufferPool.h"
#include <vector>

using namespace std;

//a null bytebuffer pool, so that we can use it later to evaluate how effective the bytebuffer pool
//is when switching between NullByteBufferPool and QueueBasedByteBufferPool.
void NullByteBufferPool::initialize() {
    //do nothing
    LOG(INFO) << "initialize null-based bytebuffer pool"<<endl;
}

  //do nothing, as no byte buffers get pooled.
void NullByteBufferPool::shutdown() {
    //do nothing.
    LOG(INFO) << "shutdown null-based bytebuffer pool"<<endl;
}   

void NullByteBufferPool::inspectBuffers() {
    //do nothing;
    LOG(INFO) << "no bytebuffer exit for inspection...." << endl;
}

void QueueBasedByteBufferPool::initialize() {
   //do nothing
   LOG(INFO) << "initialize queue-based bytebuffer pool"<<endl;
}

//this is shutdown, when the whole executor is shutdown.
void QueueBasedByteBufferPool::shutdown() {
    unsigned char *remaining=nullptr;
    vector <unsigned char*> collected_buffers;
   
    //collect the pointers first.
    while (bufferPool.try_pop (remaining)) {
      collected_buffers.push_back(remaining);
    }

    //scan the entire vector, and then free it.
    for (auto p = collected_buffers.begin(); p != collected_buffers.end(); ++p) {
        free(*p);
    }

    //at the end the queue is empty. 
    LOG(INFO) << "shutdown queue-based bytebuffer pool"<<endl;
}


//for debugging purpose. here no byte buffers exist.
void QueueBasedByteBufferPool::inspectBuffers() {
  typedef concurrent_queue<unsigned char*>::iterator iter;
  for( iter i(bufferPool.unsafe_begin()); i!=bufferPool.unsafe_end(); ++i ) {
    LOG(INFO)<< "pooled byte buffer with starting address: " << *i <<endl;
  }

}
