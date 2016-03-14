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
#include "ShuffleDataSharedMemoryManagerHelper.h"
#include "ShuffleDataSharedMemoryWriter.h"
#include "ShuffleDataSharedMemoryReader.h"
#include "EnumKvTypes.h"
#include "ShuffleConstants.h"

void ShuffleDataSharedMemoryManagerHelper::free_indexanddata_chunks (
              unsigned char *indexchunk_offset, 
              RRegion::TPtr<void> & indexchunk_globalpointer,
	      ShuffleDataSharedMemoryManager *memoryManager){

  unsigned char *p = indexchunk_offset;
  int keytypeId =ShuffleDataSharedMemoryReader::read_indexchunk_keytype_id(p);
  p+=sizeof(keytypeId);

  int kclass_size=0;
  int vclass_size=0;
  //currently we skip reading the key Value and Value Value.
  //unsigned char *kvalue=nullptr;
  //unsigned char *vvalue=nullptr;

  if ((keytypeId == KValueTypeId::Int)
      ||(keytypeId == KValueTypeId::Long)
      ||(keytypeId == KValueTypeId::Float)
      ||(keytypeId == KValueTypeId::Double)
      ||(keytypeId == KValueTypeId::String)){

    vclass_size=ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
    p+=sizeof(vclass_size);      //advance to value's first byte.

    //I now skip to read actual Key'value and Value's value. just advance the pointer
    p+=vclass_size;
  }
  else if(keytypeId == KValueTypeId::Object) {
    kclass_size=ShuffleDataSharedMemoryReader::read_indexchunk_size_keyclass(p);
    p+=sizeof(kclass_size);
    vclass_size=ShuffleDataSharedMemoryReader::read_indexchunk_size_valueclass(p);
    p+=sizeof(vclass_size);     

    //I am now skipp to read actual Key'value and Value's value. just advance the pointer
    p+=kclass_size;
    p+=vclass_size;
  }

  int totalbuckets_size=0;
  ShuffleDataSharedMemoryReader::read_indexchunk_number_total_buckets(p, &totalbuckets_size);
  p += sizeof(totalbuckets_size);//do not forget to advance the pointer for this size value.

  VLOG(2) << "to free map shuffle shm region, retrieved bucket size: " << totalbuckets_size;
  if (totalbuckets_size > 0) {
    int count = 0; 
    //free all of the data chunks, 
    while (count < totalbuckets_size) 
    {
      //NOTE: using long, because current implementation is with the virtual address pointer. 
      uint64_t gregion_id=-1; 
      uint64_t goffset=-1; 
      int bsize =0;

      ShuffleDataSharedMemoryReader::read_indexchunk_bucket(p, &gregion_id, &goffset, &bsize);
      p+= sizeof(gregion_id) + sizeof(goffset) + sizeof(bsize);

      RRegion::TPtr<void> globalpointer(gregion_id, goffset);

      if (SHMShuffleGlobalConstants::USING_RMB) {

        if (bsize > 0) {

          VLOG (2) << "map shuffle shm region, for data chunk: " << count << " size: " << bsize
                   << " at offset: " << goffset << " region id: " << gregion_id;
          VLOG (2) << "to free map shuffle shm region, for data chunk: " << count 
                   << " at offset: " << goffset << " region id: " << gregion_id;

          memoryManager->free_datachunk(globalpointer);
	}
      }
      else {
        unsigned char *ptr=reinterpret_cast<unsigned char *>(goffset);
        if (bsize > 0) { 
          VLOG (2) << "map shuffle shm region, for data chunk: " << count << " size: " << bsize
                   << " at address: " << (void*)ptr; 
          VLOG (2) << "to free map shuffle shm region, for data chunk: " << count 
                   << " at address: " << (void*)ptr; 

          memoryManager->free_datachunk(globalpointer);
        }
      }
     
      count++;
    }
  }

  //finally, we need to free the index chunk as well
 VLOG (2) << "to free map shuffle shm region, for index chunk at local address: " << (void*)indexchunk_offset;
 //indexchunk_offset is the local pointer, we need a global pointer associated with indexchunk
 memoryManager->free_indexchunk(indexchunk_globalpointer);
  
};

