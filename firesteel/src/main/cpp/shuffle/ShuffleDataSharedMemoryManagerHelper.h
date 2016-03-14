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

#ifndef SHUFFLE_DATA_SHARED_MEMORY_MANAGER_HELPER_H_
#define SHUFFLE_DATA_SHARED_MEMORY_MANAGER_HELPFER_H_

#include "ShuffleDataSharedMemoryManager.h"

//the other one: RmbShuffleDataSharedMemoryManager
class ShuffleDataSharedMemoryManagerHelper {
  public: 

  //the shuffle manager will issue the free of NVM. When it is freed
  //indexchunk_offset records the local pointer of index chunk
  //indexchunk_globalpointer records the global pointer of index chunk.
  static void free_indexanddata_chunks (unsigned char *indexchunk_offset,
                                        RRegion::TPtr<void> & indexchunk_globalpointer,
                                        ShuffleDataSharedMemoryManager *memoryManager);

};

#endif 
