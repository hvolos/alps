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

#ifndef _ALPS_PERSIST_HH_
#define _ALPS_PERSIST_HH_

#ifdef __ARCH_NON_VOLATILE__
#include <libpmem.h>

static inline void persist(void *addr, size_t len)
{
    pmem_persist(addr, len);
}

#else
static inline void persist(void *addr, size_t len)
{
    // do nothing
    return;
}

#endif

#endif // _ALPS_PERSIST_HH_
