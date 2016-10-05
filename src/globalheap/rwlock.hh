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

#ifndef _ALPS_RWLOCK_HH_
#define _ALPS_RWLOCK_HH_

#include <pthread.h>

namespace alps {

#define NUM_MUTEX 8

class ReaderWriterLock {
public:
    ReaderWriterLock()
    {
        for (int i=0; i<NUM_MUTEX; i++) {
            pthread_mutex_init(&mutex_[i], NULL);
        }
    }


    void lock_read();
    void lock_write();
    void unlock_read();
    void unlock_write();

private:
    pthread_mutex_t mutex_[NUM_MUTEX];
};

inline void ReaderWriterLock::lock_read()
{
    int i = pthread_self() % NUM_MUTEX;
    pthread_mutex_lock(&mutex_[i]);
}

inline void ReaderWriterLock::unlock_read()
{
    int i = pthread_self() % NUM_MUTEX;
    pthread_mutex_unlock(&mutex_[i]);
}

inline void ReaderWriterLock::lock_write()
{
    for (int i=0; i<NUM_MUTEX; i++) {
        pthread_mutex_lock(&mutex_[i]);
    }
}

inline void ReaderWriterLock::unlock_write()
{
    for (int i=0; i<NUM_MUTEX; i++) {
        pthread_mutex_unlock(&mutex_[i]);
    }
}


} // namespace alps

#endif // _ALPS_RWLOCK_HH_
