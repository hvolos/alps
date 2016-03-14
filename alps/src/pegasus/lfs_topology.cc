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

#include <numa.h>
#include <unistd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */

#include <iostream>

#include "common/assorted_func.hh"
#include "common/os.hh"
#include "pegasus/lfs_topology.hh"

namespace alps {

Topology* LfsTopology::construct(const boost::filesystem::path& pathname, const PegasusOptions& pegasus_options) 
{
    return new LfsTopology(pathname, pegasus_options);
}

LfsTopology::LfsTopology(const boost::filesystem::path& pathname, const PegasusOptions& pegasus_options)
    : last_is_valid_(false)
{
    node_running_on();
}

InterleaveGroup LfsTopology::max_interleave_group()
{
    assert(0 && "functionality not implemented");
    return 0;
}

unsigned int LfsTopology::node_running_on()
{
    assert(0 && "functionality not implemented");
    return 0;
}


InterleaveGroup LfsTopology::nearest_ig()
{
    assert(0 && "functionality not implemented");
    return 0;
}

LfsTopology::~LfsTopology()
{
    // nothing to do
}

} // namespace alps
