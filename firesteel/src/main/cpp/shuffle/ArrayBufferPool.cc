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
#include "ArrayBufferPool.h"
#include <vector>

using namespace std;

//a null ArrayBuffer pool, so that we can use it later to evaluate how effective the array-buffer pool
//is when switching between NullArrayBufferPool and QueueBasedArrayBufferPool.
void NullArrayBufferPool::initialize() {
    //do nothing
    LOG(INFO) << "initialize null-based array-buffer pool"<<endl;
}

//do nothing, as no array-buffers get pooled.
void NullArrayBufferPool::shutdown() {
    //do nothing.
    LOG(INFO) << "shutdown null-based array-buffer pool"<<endl;
}   

void NullArrayBufferPool::inspectBuffers() {
    //do nothing;
    LOG(INFO) << "no array-buffer exit for inspection...." << endl;
}

void QueueBasedArrayBufferPool::initialize() {
   //do nothing
   LOG(INFO) << "initialize queue-based array-buffer pool"<<endl;
}

//this is shutdown, when the whole executor is shutdown.
void QueueBasedArrayBufferPool::shutdown() {
    vector <ArrayBufferElement> collected_buffers;
    {
       ArrayBufferElement element;
       while (bufferPool.try_pop(element)) {
   	  collected_buffers.push_back(element);
       }
    }

    //scan the entire vector, and then free it.
    for (auto p = collected_buffers.begin(); p != collected_buffers.end(); ++p) {
      free((*p).start_address);
    }

    //at the end the queue is empty. 
    LOG(INFO) << "shutdown queue-based array-buffer pool"<<endl;
}


//for debugging purpose. here no byte buffers exist.
void QueueBasedArrayBufferPool::inspectBuffers() {
  typedef concurrent_queue<ArrayBufferElement>::iterator iter;
  for( iter i(bufferPool.unsafe_begin()); i!=bufferPool.unsafe_end(); ++i ) {
    LOG(INFO)<< "pooled array buffer with starting address: " << (void*)(*i).start_address <<endl;
  }
}
