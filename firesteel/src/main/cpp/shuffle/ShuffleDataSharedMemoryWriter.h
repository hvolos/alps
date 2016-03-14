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

#ifndef SHUFFLE_DATA_SHARED_MEMORY_WRITER_H
#define SHUFFLE_DATA_SHARED_MEMORY_WRITER_H

#include <string.h>
#include <stdlib.h>

using namespace std;


/*
 *How index chunk and data chunks are allocated from shared-memory 
 *is done via ShuffleDataSharedMemoryManager.
 *
 */
class ShuffleDataSharedMemoryWriter {

 public:

      ShuffleDataSharedMemoryWriter() {
        //do nothing.
      }
 
      //void *memcpy(void *dest, const void *src, size_t n);
      //write index chunk: Key Type Id 
      unsigned char *write_indexchunk_keytype_id(unsigned char *p, int id) {
	int *q=(int*)p;
	*q = id;
	return(p+=sizeof(id));
      }

      //write index chunk: size of the key class definition 
      unsigned char *write_indexchunk_size_keyclass (unsigned char *p, int kcsize) {
	int *q = (int*)p;
	*q = kcsize;
	return (p+=sizeof(kcsize));
      }

      //write index chunk: size of the value class definition 
      unsigned char *write_indexchunk_size_valueclass(unsigned char  *p, int vcsize) {
	int *q = (int*)p;
        *q = vcsize;
        return (p+= sizeof(vcsize));
      }

      unsigned char *write_indexchunk_keyclass (unsigned char *p, 
                                         unsigned char *definition, int size) {
	memcpy(p, definition, size);
        return (p+=size);
      }

      unsigned char  *write_indexchunk_valueclass (unsigned char *p,
                                         unsigned char *definition, int size) {
        memcpy(p, definition, size);
        return (p+=size);
      }

      unsigned char  *write_indexchunk_number_total_buckets(unsigned char *p, int tsize) {
	int *q = (int*)p;
        *q = tsize;
        return (p+=sizeof(tsize));
      }

      //the following method will be invoked, after each data chunk is writen.
      //the global pointer for a bucket is represented as <region id, offset>
      unsigned char  *write_indexchunk_bucket(unsigned char  *p, uint64_t region_id, 
                                                        uint64_t offset, int bsize) {
	uint64_t *q = (uint64_t*)p;

        *q = region_id;
        p+=sizeof(region_id);

        uint64_t *r = (uint64_t*) p;
        *r = offset;
        p+=sizeof(offset);

        int *s = (int*)p;
        *s = bsize;
        p+=sizeof(bsize);

        return p;
      }
       
      //write data chunk, for keys with int values.
      unsigned char *write_datachunk_intkey (unsigned char  *p, int kvalue, int vvaluesize) {
	int *q = (int*)p;
        *q = kvalue;
        p+=sizeof(kvalue);

        int *s = (int*)p;
        *s = vvaluesize;
        p+=sizeof(vvaluesize);

        return p;
      };


      unsigned char  *write_datachunk_floatkey ( unsigned char *p, float kvalue, int vvaluesize) {
	float *q = (float*)p;
        *q=kvalue;
        p+=sizeof(kvalue);

        int *s = (int*)p;
        *s = vvaluesize;
        p+=sizeof(vvaluesize);

        return p;

      };

      //write data chunk, for keys with long values.
      unsigned char  *write_datachunk_longkey ( unsigned char *p, long kvalue, int vvaluesize) {
	long *q = (long*)p;
        *q = kvalue;
        p+=sizeof(kvalue);

        int *s = (int*)p;
        *s = vvaluesize;
        p+=sizeof(vvaluesize);

        return p;

      };

      //to write data that goes beyond a single byte buffer
      unsigned char *write_datachunk_value (unsigned char *p, unsigned char *vvalue, int size ) {
	memcpy(p, vvalue, size);
        p+=size;
        return p;
      }

      //to write normalized key, if the key is no larger than the maximum normalized key size
      unsigned char *write_datachunk_normalizedstringkey (unsigned char *p, long *normalizedKey, int normalized_size) {
	long *q = (long*) p;
        *q = *normalizedKey;
        p+= normalized_size;
        return p;
      }


      //write data chunk, for keys with float values.

      //write data chunk, for keys with string  values. for better reading, we will need to 
      //write the key's size and then the value's size, then the Key's value and Value's value.
      //WARNING: this needs to be changed to avoid zero copy from bytebuffer manager's managed 
      //byte buffer.
      unsigned char  *write_datachunk_stringkey (unsigned char *p, int  kvalue_size,
                        unsigned char *kvalue, int vvaluesize, unsigned char *vvalue) {
	int *q = (int*) p;
	*q = kvalue_size;
	p+=sizeof(kvalue_size);

	int *s = (int*)p;
	*s = vvaluesize;
	p+=sizeof(vvaluesize);

	//then the Key's value	
        memcpy(p, kvalue, kvalue_size);
	p+=kvalue_size;
	//then the Value's value	
        memcpy(p, vvalue, vvaluesize);
	p+= vvaluesize;

	return p;
      };


      //write data chunk, for keys with object  values.
      unsigned char* write_datachunk_objectkey (unsigned char *p, int  kvalue_size, 
                    unsigned char *kvalue, int vvaluesize, unsigned char *vvalue) {
        int *q = (int*)p;
	*q = kvalue_size;
	p+=sizeof(kvalue_size);

	int *s = (int*)p;
	*s = vvaluesize;
	p+=sizeof(vvaluesize);

	//then the Key's value	
        memcpy(p, kvalue, kvalue_size);
	p+=kvalue_size;
	//then the Value's value	
        memcpy(p, vvalue, vvaluesize);
	p+= vvaluesize;

	return p;

      };

};

#endif 
