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

#ifndef _ALPS_TEST_FS_FIXTURE_HH_
#define _ALPS_TEST_FS_FIXTURE_HH_

#include <unistd.h>
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "pegasus/lfs_region_file.hh"
#include "pegasus/pegasus_options.hh"
#include "pegasus/tmpfs_region_file.hh"


class TmpfsConfig {
protected:
    std::string testdir = "/dev/shm/nvm/";
    //size_t      booksize = 8*1024*1024LLU*1024LLU;
    size_t      booksize = 8*1024LLU*1024LLU; // set a smaller booksize to make testing faster

    std::string test_path(const char* name) {
        return testdir + std::string(name);
    }

    void set_booksize(size_t sz = 0) {
        if (!sz) {
            sz = booksize;
        }
        alps::TmpfsRegionFile::kBookSize = booksize;
    }
};

class LfsConfig {
protected:
    std::string testdir = "/lfs/";
    //size_t      booksize = 8*1024LLU*1024LLU*1024LLU; 
    size_t      booksize = 8*1024LLU*1024LLU; // set a smaller booksize to make testing faster

    std::string test_path(const char* name) {
        return testdir + std::string(name);
    }

    void set_booksize(size_t sz = 0) {
        if (!sz) {
            sz = booksize;
        }
        alps::LfsRegionFile::kBookSize = booksize;
    }
};


class TmpfsRegionFileTest : public ::testing::Test, public TmpfsConfig {
protected:

    void reset() {
        char buf[1024];

        sprintf(buf, "mkdir -p %s", testdir.c_str());
        system(buf);
        sprintf(buf, "rm -rf %s/*", testdir.c_str());
        system(buf);
        system("rm -f /dev/shm/@@lockfile@@");
        system("umask 0");
        system("touch /dev/shm/@@lockfile@@");
    }

    void SetUp() 
    {
        alps::TmpfsRegionFile::kBookSize = booksize;
        pegasus_options.tmpfs_options.booksize = booksize;
        reset();
    }

    alps::PegasusOptions pegasus_options;
};


class LfsRegionFileTest : public ::testing::Test, public LfsConfig {
protected:
    void reset() {
        char buf[1024];

        sprintf(buf, "rm -rf %s/*", testdir.c_str());
        system(buf);
    }

    void SetUp() 
    {
        alps::LfsRegionFile::kBookSize = booksize;
        pegasus_options.lfs_options.booksize = booksize;
        reset();
    }

    alps::PegasusOptions pegasus_options;
};

#if USE_TMPFS_NVMFILE
typedef TmpfsRegionFileTest RegionFileTest;
typedef TmpfsEnvironment FsEnvironment;
#endif
#if USE_LFS_NVMFILE
typedef LfsRegionFileTest RegionFileTest;
typedef LfsEnvironment FsEnvironment;
#endif


#endif // _ALPS_TEST_FS_FIXTURE_HH_
