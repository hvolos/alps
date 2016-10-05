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
#include <sys/syscall.h>   

#include <iostream>

#include "common/assorted_func.hh"
#include "common/os.hh"
#include "pegasus/tmpfs_topology.hh"

namespace alps {

Topology* TmpfsTopology::construct(const boost::filesystem::path& pathname, const PegasusOptions& pegasus_options) 
{
    return new TmpfsTopology(pathname, pegasus_options);
}

TmpfsTopology::TmpfsTopology(const boost::filesystem::path& pathname, const PegasusOptions& pegasus_options)
    : last_node_is_valid_(false)
{
    // initialize node running on 
    node_running_on();
}

InterleaveGroup TmpfsTopology::max_interleave_group()
{
    return numa_max_node();
}

unsigned int TmpfsTopology::node_running_on()
{
    if (last_node_is_valid_) {
        return last_node_;
    }

    unsigned int cpu;
    unsigned int node;

    ASSERT_ND(syscall(SYS_getcpu, &cpu, &node, NULL) == 0);
    last_node_ = node;
    last_node_is_valid_ = true;
    return last_node_;
}

int TmpfsTopology::run_on_node(int node)
{
    int ret;
    if ((ret = numa_run_on_node(node)) < 0) {
        return ret;
    }
    last_node_is_valid_ = false;
    return 0;
}

InterleaveGroup TmpfsTopology::nearest_ig()
{
    return node_running_on();
}

TmpfsTopology::~TmpfsTopology()
{
    // nothing to do
}

} // namespace alps
