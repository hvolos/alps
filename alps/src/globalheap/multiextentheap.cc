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
 */

#include "globalheap/multiextentheap.hh"

namespace alps {

MultiExtentHeap::MultiExtentHeap(RRegion::TPtr<nvHeap> nvheap)
    : nvheap_(nvheap)
{ 
    assert(pthread_mutex_init(&mutex_, 0) == 0);
}

ExtentHeap* MultiExtentHeap::find_or_bind(const MemAttrib& memattrib)
{
    ExtentHeap* extentheap = extentheaps_.find(memattrib);
    if (!extentheap) {
        extentheap = new ExtentHeap(nvheap_);
        extentheap->init();
        extentheaps_.bind(memattrib, extentheap);
    }
    assert(extentheap != NULL);
    return extentheap;
}



} // namespace alps
