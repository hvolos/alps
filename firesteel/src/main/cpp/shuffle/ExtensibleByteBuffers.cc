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
#include "ExtensibleByteBuffers.h"
#include "ByteBufferHolder.h"
#include <string.h>
#include <fstream>

PositionInExtensibleByteBuffer NonExtensibleByteBuffer:: append(unsigned char*buffer, size_t size){
  PositionInExtensibleByteBuffer coordinate; 

  if (bufferHolder.remain_capacity() == 0 ) {
     //it is to be overflowed.
     overflowed = true; 
     coordinate.start_buffer =0; //I only have one buffer
     coordinate.position_in_start_buffer= -1;
     coordinate.value_size = -1; 
     return coordinate; 
  }

  //I only have one buffer
  coordinate.start_buffer = 0;
  coordinate.position_in_start_buffer =bufferHolder.position_in_buffer();
  coordinate.value_size = size; 

  
  if (bufferHolder.remain_capacity() > size) {
       bufferHolder.append_inbuffer(buffer, size); 
  }
  else {
     overflowed = true; 
     coordinate.position_in_start_buffer= -1;
     coordinate.value_size = -1; 
  }

  return coordinate; 
}

//a very simple buffer to buffer copy.
void NonExtensibleByteBuffer::retrieve (
        const PositionInExtensibleByteBuffer &posBuffer, unsigned char* buffer) {
  unsigned char *srcBuffer = bufferHolder.position_at(posBuffer.position_in_start_buffer);
  memcpy (buffer, srcBuffer, posBuffer.value_size);
}


inline int NonExtensibleByteBuffer::retrieve (const PositionInExtensibleByteBuffer &posBuffer, 
                      	      unsigned char *buffer, int maximum_size)  {
  int sizeCopied =
             (maximum_size > posBuffer.value_size)? maximum_size: posBuffer.value_size;
  unsigned char *srcBuffer = bufferHolder.position_at(posBuffer.position_in_start_buffer);
  memcpy (buffer, srcBuffer, sizeCopied);
  return sizeCopied;
}

PositionInExtensibleByteBuffer ExtensibleByteBuffers:: append(unsigned char*buffer, size_t size){
  PositionInExtensibleByteBuffer coordinate; 

  if (current_buffer().remain_capacity() == 0 ) {
    create_or_advanceto_next_buffer();
  }

  //be careful: currentHolder can not be re-assigned, as otherwise, it will lead to
  //the change to the source.
  ByteBufferHolder& currentHolder = current_buffer();
  coordinate.start_buffer = current_buffer_position(); 
  coordinate.position_in_start_buffer =currentHolder.position_in_buffer();
  coordinate.value_size = size; 

  if (currentHolder.remain_capacity() > size) {
     currentHolder.append_inbuffer(buffer, size); 
  }
  else {
    size_t remain_capacity = currentHolder.remain_capacity();

    if (remain_capacity > 0) {
      currentHolder.append_inbuffer(buffer, remain_capacity); 
    }

    int remsize = size-remain_capacity;
    unsigned char *rembuffer = buffer + remain_capacity; 

    //we may need to span more than one full buffer in some unsual case.
    while (remsize > bufsize) {
      //be careful: currentHolder can not be re-assigned, as otherwise, it will lead to
      //the change to the source.
      ByteBufferHolder& holder = create_or_advanceto_next_buffer();
      holder.append_inbuffer(rembuffer, bufsize);
      rembuffer += bufsize;
      remsize -= bufsize; 
      
    }    

    if (remsize > 0) {
      //last part, then we are done
      ByteBufferHolder &finholder =create_or_advanceto_next_buffer();
      finholder.append_inbuffer(rembuffer, remsize);
    }
    
  }

  return coordinate; 
}


//the client will make sure that the buffer size is bigger enough to hold all data. since this is performance
//critial, we make it inline.
inline void ExtensibleByteBuffers::retrieve (
            const PositionInExtensibleByteBuffer &posBuffer, unsigned char* buffer) {
  int start_buffer = posBuffer.start_buffer;
  ByteBufferHolder &startHolder = buffer_at(start_buffer);
  int totalSize= posBuffer.value_size;
  int capacity = startHolder.capacity();

  unsigned char *srcBuffer = startHolder.position_at(posBuffer.position_in_start_buffer);

  if (posBuffer.position_in_start_buffer + totalSize <= capacity) {
    //can  be held in the first buffer.                                                                                                       
    //ptr = writer.write_datachunk_intkey(ptr, p->key, , srcBuffer);
    memcpy (buffer, srcBuffer, posBuffer.value_size);
  }
  else {
    //get the first buffer
    int firstSegmSize = capacity - posBuffer.position_in_start_buffer;

    //ptr = writer.write_datachunk_intkey(ptr, p->key, firstSegmSize, srcBuffer);
    memcpy(buffer, srcBuffer, firstSegmSize);
    buffer+=firstSegmSize;

    int remainingSize= totalSize - firstSegmSize;

    //we will span across multiple byte holders.                                                                                              
    while (remainingSize >= capacity) {
      start_buffer++;
      ByteBufferHolder &currentHolder = buffer_at(start_buffer);
      //ptr = writer.write_datachunk_remainingbytes(ptr, currentHolder.position_at(0), capacity);
      memcpy(buffer, currentHolder.position_at(0), capacity);
      buffer+=capacity;
      remainingSize -= capacity;
    }

    //the last one, remaining size is in the last holder
    if (remainingSize > 0) {
      start_buffer++;
      ByteBufferHolder& lastHolder = buffer_at(start_buffer);
      //ptr = writer.write_datachunk_remainingbytes(ptr, currentHolder.position_at(0), remainingSize);
      memcpy(buffer, lastHolder.position_at(0), remainingSize); 
      buffer+=remainingSize;
    }

  }
  
}

//to allow debugging by inspecting actual byte elements in the buffer.
void ExtensibleByteBuffers::writeToFile (
           const string &fileName, const PositionInExtensibleByteBuffer &posBuffer) {
  ofstream log_file(fileName, std::ios_base::out | std::ios_base::app );
  int start_buffer = posBuffer.start_buffer;
  log_file << "start_buffer is: " << start_buffer << endl;

  log_file << "position in start_buffer is: " << posBuffer.position_in_start_buffer  << endl;

  ByteBufferHolder &startHolder = buffer_at(start_buffer);
  int totalSize= posBuffer.value_size;
  log_file << "total size to retrieved is: " << totalSize << endl;

  int capacity = startHolder.capacity();
  log_file << "corresponding buffer capacity is: " << capacity << endl;

  unsigned char *srcBuffer = startHolder.position_at(posBuffer.position_in_start_buffer);

  int counter=0;
  if (posBuffer.position_in_start_buffer + totalSize <= capacity) {
    //can  be held in the first buffer.                                                                                                        
    //ptr = writer.write_datachunk_intkey(ptr, p->key, , srcBuffer);
    //memcpy (buffer, srcBuffer, posBuffer.value_size);

    log_file << "read data only from first buffer..............: " << endl;
    for (int i=0; i<posBuffer.value_size; i++) {
      unsigned char v= *(srcBuffer+i);
   
      log_file << "****retrieved buffer*** " << " position: " << counter << " with value: " << (int) v 
               << " located at buffer index:" << start_buffer <<endl;
      counter ++;
    }
  }
  else {
    //get the first buffer                                                                                                                     
    int firstSegmSize = capacity - posBuffer.position_in_start_buffer;

    //ptr = writer.write_datachunk_intkey(ptr, p->key, firstSegmSize, srcBuffer);
    //memcpy(buffer, srcBuffer, firstSegmSize);
    {
      for (int i=0; i<firstSegmSize; i++) {
        unsigned char v= *(srcBuffer+i);
   
        log_file << "****retrieved buffer**** " << " position: " << counter << " with value: " << (int) v 
               << " located at buffer index:" << start_buffer <<endl;
        counter ++;
      }
    }
    //buffer+=firstSegmSize;

    int remainingSize= totalSize - firstSegmSize;

    //we will span across multiple byte holders.                                                                                               
    while (remainingSize >= capacity) {
      start_buffer++;
      ByteBufferHolder &currentHolder = buffer_at(start_buffer);
      //ptr = writer.write_datachunk_remainingbytes(ptr, currentHolder.position_at(0), capacity);
      //memcpy(buffer, currentHolder.position_at(0), capacity);
      srcBuffer=currentHolder.position_at(0);
      {
        for (int i=0; i<capacity; i++) {
           unsigned char v= *(srcBuffer+i);
   
           log_file << "****retrieved buffer*** " <<  " position: " << counter << " with value: " << (int) v 
             << " located at buffer index:" << start_buffer <<endl;

           counter ++;
	}
      }
      //buffer+=capacity;
      remainingSize -= capacity;
    }

    //the last one, remaining size is in the last holder                                                                                       
    if (remainingSize >0) {
      start_buffer++;
      ByteBufferHolder &currentHolder = buffer_at(start_buffer);
      //ptr = writer.write_datachunk_remainingbytes(ptr, currentHolder.position_at(0), remainingSize);
      //memcpy(buffer, currentHolder.position_at(0), remainingSize); 
      srcBuffer=currentHolder.position_at(0);
      {
        for (int i=0; i<remainingSize; i++) {
           unsigned char v= *(srcBuffer+i);
   
           log_file << "****retrieved buffer*** " << " position: " << counter << " with value: " << (int) v
             << " located at buffer index:" << start_buffer <<endl;
           counter ++;
	}
      }

      //buffer+=remainingSize;
    }

  }
}

//reason: this method is used for normalized key for merge-sort, and reduce side used non-extensible
//bytebuffer. Thus we can avoid invoking this method.
int ExtensibleByteBuffers::retrieve (const PositionInExtensibleByteBuffer &posBuffer, 
                       	      unsigned char *buffer, int maximum_size)  {
  return 0;
}


