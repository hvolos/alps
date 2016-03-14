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

#ifndef SHUFFLE_DATA_SHARED_MEMORY_READER_H
#define SHUFFLE_DATA_SHARED_MEMORY_READER_H

#include <string.h>
#include <stdlib.h>
#include "ExtensibleByteBuffers.h"
using namespace std;


/*
 *to Reader index chunk and data chunks allocated from shared-memory 
 *
 */
class ShuffleDataSharedMemoryReader {
 private:
     int regionId; 
 public:

      ShuffleDataSharedMemoryReader (int regId): regionId(regId) {

      }
      //void *memcpy(void *dest, const void *src, size_t n);

      //read index chunk: Key Type Id, the caller will need to advance the pointer by itself
      static int read_indexchunk_keytype_id(unsigned char *p) {
        return  (*((int*)p) );
      }

      //read ndex chunk: size of the key class definition 
      static int read_indexchunk_size_keyclass (unsigned char *p) {
        return (*((int*)p));
      }

      //write index chunk: size of the value class definition 
      static int read_indexchunk_size_valueclass(unsigned char *p) {
        return (*((int*)p));
      }

      //the caller will need to create the holder to hold the definition with the corresponding size.
      static void read_indexchunk_keyclass (unsigned char *p, unsigned char *definition, int size) {
	memcpy(definition, p, size);
      }

      static void read_indexchunk_valueclass (unsigned char *p, unsigned char *definition, int size) {
        memcpy(definition, p, size);
      }

      //first, we need to have the total size of the buckets, that include zero-sized buckets
      static void read_indexchunk_number_total_buckets(unsigned char *p, int *tsize) {
        *tsize = *((int*)p);
      }

      //the following method will be invoked, after each data chunk is writen.
      //it records the global pointer and size of each data chunk.
      static void read_indexchunk_bucket(unsigned char *p, uint64_t *region_id,
                                                           uint64_t *offset, int *bsize) {
        *region_id = *((uint64_t*)p);
        *offset = *((uint64_t*)(p + sizeof(*region_id)));

        int *s = (int*)(p + sizeof(*region_id) + sizeof(*offset));
        *bsize = *s;
      }
       
      //read data chunk, for keys with int values.
      static void read_datachunk_intkey (unsigned char *p, int *kvalue, int *vvaluesize) {
	*kvalue = *((int*)p);
        int *s= (int*)(p+ sizeof(*kvalue));

        *vvaluesize = *s;
      };
    
      static void read_datachunk_value (unsigned char *p, unsigned char *vvalue, int size) {
	memcpy (vvalue, p, size);
      }

      static void read_datachunk_keyassociated_value (unsigned char *p, 
                                                   int vvaluesize, unsigned char *vvalue) {
        memcpy(vvalue, p, vvaluesize);
      };

      //overloading method, to directly copy to the byte buffer. 
      static PositionInExtensibleByteBuffer read_datachunk_keyassociated_value(unsigned char *p, int vvaluesize,
		       BufferManager *bufferMgr) {
	  PositionInExtensibleByteBuffer result = bufferMgr->append(p, vvaluesize);
	  return result; 
      }


      //write data chunk, for keys with float values.
      static void  read_datachunk_floatkey (unsigned char *p, float *kvalue, int *vvaluesize){
	*kvalue = *((float*)p);
        int *s=(int*)(p+sizeof(*kvalue));

        *vvaluesize = *s;

      };

      static void read_datachunk_floatkey_value (unsigned char *p, int vvaluesize,
                                                                   unsigned char *vvalue) {
        memcpy(vvalue, p, vvaluesize);
      };


      //write data chunk, for keys with long values.
      static void  read_datachunk_longkey (unsigned char *p, long *kvalue, int *vvaluesize){
	*kvalue = *((long*)p);
        int *s=(int*)(p+ sizeof(*kvalue));

        *vvaluesize = *s;
      };

      static void read_datachunk_longkey_value (unsigned char *p, int vvaluesize,
                                                                unsigned char *vvalue) {
        memcpy(vvalue, p, vvaluesize);
      };


      //read data chunk, for key size and then value size with string  values.
      static void read_datachunk_stringkey_kvsizes (unsigned char *p, int  *kvalue_size,
                                                                        int *vvaluesize) {
	*kvalue_size = *((int*)p);
	int *s= (int*)(p+ sizeof(*kvalue_size));

	*vvaluesize = *s;
      };

      //read data chunk, for Key's value and then Value's value.
      static void read_datachunk_stringkey_kvvalues (unsigned char *p,  int kvalue_size,
                                             unsigned char *kvalue,
					     int vvaluesize, unsigned char *vvalue){
	memcpy(kvalue, p, kvalue_size);
        memcpy(vvalue, p + kvalue_size, vvaluesize);
      }

      //read data chunk, for key size and then value size with string  values.
      static void read_datachunk_objectkey_kvsizes (unsigned char *p, int  *kvalue_size, 
                                                                       int *vvaluesize) {
	*kvalue_size = *((int*)p);
	int *s=(int*)(p+sizeof(*kvalue_size));

	*vvaluesize=*s;
      };

      //read data chunk, for Key's value and then Value's value.
      static void read_datachunk_objectkey_kvvalues(unsigned char *p,  int kvalue_size,
                                             unsigned char *kvalue,
					     int vvaluesize, unsigned char *vvalue){
	memcpy(kvalue, p, kvalue_size);
        memcpy(vvalue, p + kvalue_size, vvaluesize);
      };

};

#endif 
