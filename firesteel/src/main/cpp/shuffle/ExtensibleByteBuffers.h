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

#ifndef EXTENSIBLEBYTEBUFFERS_H_
#define EXTENSIBLEBYTEBUFFERS_H_

#include "ByteBufferHolder.h"
#include <vector>
#include <string> 

using namespace std;

struct PositionInExtensibleByteBuffer {
  int start_buffer;
  int position_in_start_buffer;
  int value_size;  

  PositionInExtensibleByteBuffer():
	             start_buffer(0), position_in_start_buffer(0), value_size(0) {

  }

  PositionInExtensibleByteBuffer(int start, int position, int size) :
	  start_buffer(start), position_in_start_buffer(position),
	  value_size(size) {

  }
};

//define a generic byte manager that encapsulates the extensible bytebuffers and 
//non extensible bytebuffers.
class BufferManager {
 public: 
    //to append data into the internal buffer.
    virtual  PositionInExtensibleByteBuffer append(unsigned char*buffer, size_t size) =0;
    virtual  void retrieve (const PositionInExtensibleByteBuffer &posBuffer, unsigned char *buffer) =0;
    //retrieve bytes into the buffer, with maximum size spedified, and the returned is the 
    //actual bytes retrieved. 
    virtual int retrieve (const PositionInExtensibleByteBuffer &posBuffer, 
		           unsigned char *buffer, int maximum_size) =0;
    //to check that the buffer manager has external buffers exceeded the limit.
    //for extensible buffer, it is always false. for non-extensible byte buffer, it depends on actual execution.
    virtual  bool buffer_exceeded () =0;
    virtual  void reset () =0;
    virtual  void free_buffers () =0;
    virtual  ByteBufferHolder& current_buffer() =0;
    virtual ~BufferManager() {
      //do nothing
    }
};

class NonExtensibleByteBuffer: public BufferManager{
  private:
     ByteBufferHolder bufferHolder;
     bool overflowed;

  public: 
      //just take the existing byte buffer
     NonExtensibleByteBuffer (unsigned char *tbuffer, int tcapacity):
       bufferHolder(tbuffer, tcapacity), overflowed(false) {
     }

     virtual  ~NonExtensibleByteBuffer() {
         //do nothing.
     };

     
  public: 
     bool buffer_exceeded  () override {
       return overflowed; 
     }

     //to reset to the first buffer and pointer in the first buffer to zero.
     void reset() override {
        bufferHolder.reset();
     }

     ByteBufferHolder& current_buffer() override {
       return bufferHolder;
     }

     //free the buffer
     void free_buffers() override {
       //since the buffer is not self-created, the buffer holder will not 
       //free the pointer that it holds.
       bufferHolder.free_buffer();
     }

     PositionInExtensibleByteBuffer append(unsigned char*buffer, size_t size) override; 

     //retrieve the full length of bytes, specified in posBuffer, to the provided buffer
     void retrieve (const PositionInExtensibleByteBuffer &posBuffer, unsigned char *buffer) override;

     //retrieve bytes into the buffer, with maximum size spedified, and the returned is the 
     //actual bytes retrieved. 
     int retrieve (const PositionInExtensibleByteBuffer &posBuffer, 
                                      unsigned char *buffer, int maximum_size) override;
};


class  ExtensibleByteBuffers: public BufferManager {
 private:
   vector <ByteBufferHolder> buffers; 
   int bufsize;
   int curbuffer;
 
 public: 

   //pass-in buffer size as parameter 
   ExtensibleByteBuffers (size_t bsize):bufsize(bsize), curbuffer(0) {
     //so that current_buffer is at 0.
     ByteBufferHolder nholder(bufsize);
     buffers.push_back(nholder);
   };

   virtual  ~ExtensibleByteBuffers() {
     //do nothing.
   };

   size_t buffer_size() const {
     return bufsize;
   };

   //create a new buffer if the buffer pool is exhaused. or otherwise, 
   //simply re-use the buffer pool.
   ByteBufferHolder & create_or_advanceto_next_buffer() {
     int totalSize = buffers.size();
     if (curbuffer == totalSize-1) {
        // I am the last one here, I will have to create a new one.
        ByteBufferHolder nholder(bufsize);
        buffers.push_back(nholder);
        curbuffer++;
        return buffers.back();
     }
     else{
       //just advance to next one
       curbuffer++;
       return buffers[curbuffer]; 
     }
   }

   ByteBufferHolder& buffer_at(int i) {
     //operator [] does not provide bound check. 
     return buffers[i];
   }

   ByteBufferHolder& current_buffer() {
     //operator [] does not provide bound check. 
     return buffers[curbuffer];
   }

   int current_buffer_position() const {
     return curbuffer;
   }

   void free_buffers() {
     for (auto p= buffers.begin(); p!=buffers.end(); ++p) {
       p->free_buffer();
     }

     //to free the whole memory occupied by vector element. we need to
     //invoke clear();
     buffers.clear();
     curbuffer=0;
   }

   //to reset to the first buffer and pointer in the first buffer to zero.
   void reset() override {
      curbuffer=0;
      buffers[0].reset();
   }

   //to append the byte array into the buffer holder, that can                             
   //potentiallly may span multiple holders and thus require                               
   //buffer manager to be involved.                                                        
   PositionInExtensibleByteBuffer append(unsigned char*buffer, size_t size) override ;

   //for debugging, to inspect byte element specified by the position buffer. 
   void writeToFile (const string &fileName, const PositionInExtensibleByteBuffer &posBuffer);
   
   //retrieve the data specified in the position buffer into the passed-in content buffer. 
   //the pass-in content buffer is pre-allocated with the buffer size specified by the position buffer
   void retrieve (const PositionInExtensibleByteBuffer &posBuffer, unsigned char *buffer) override;
   //to free all of the buffers and the encapsulate allocated memory.

   //retrieve bytes into the buffer, with maximum size spedified, and the returned is the 
   //actual bytes retrieved. 
   int retrieve (const PositionInExtensibleByteBuffer &posBuffer, 
                                      unsigned char *buffer, int maximum_size) override;
   bool buffer_exceeded () override {
      return false; //as we can always grow.
   }

};

#endif 
