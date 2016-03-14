# * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
#

import subprocess
import re

def num_numa_nodes():
    p = subprocess.Popen(['numactl', '-H'], stdout=subprocess.PIPE, 
                                            stderr=subprocess.PIPE)

    out, err = p.communicate()
    for line in out.split('\n'):
        if re.search("available", line):
            return int(line.split(':')[1].lstrip().split(' ')[0])

def numcores():
    p = subprocess.Popen(['lscpu'], stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)

    out, err = p.communicate()
    for line in out.split('\n'):
        if re.search("On-line CPU\(s\)", line):
            cpu_list = line.split(':')[1].lstrip()
            cpu_low = int(cpu_list.split("-")[0])
            cpu_high = int(cpu_list.split("-")[1])
            return cpu_high-cpu_low+1		#Warning: This will only work if there is a single range of on-line cores. E.g., won't work for something like 0-5, 7-15

def numsockets():
    p = subprocess.Popen(['lscpu'], stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)

    out, err = p.communicate()
    for line in out.split('\n'):
        if re.search("Socket\(s\)", line):
            return int(line.split(':')[1].lstrip())


def numthreadspercore():
    p = subprocess.Popen(['lscpu'], stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)

    out, err = p.communicate()
    for line in out.split('\n'):
        if re.search("Thread\(s\) per core", line):
            return int(line.split(':')[1].lstrip())

def numcorespersocket():
    p = subprocess.Popen(['lscpu'], stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)

    out, err = p.communicate()
    for line in out.split('\n'):
        if re.search("Core\(s\) per socket", line):
            return int(line.split(':')[1].lstrip())

# find a node with available memory and assign it to vnode_id
def vnode_to_memnode(vnode_id):
    p = subprocess.Popen(['numactl', '-H'], stdout=subprocess.PIPE, 
                                            stderr=subprocess.PIPE)
    node_list = []
    out, err = p.communicate()
    for line in out.split('\n'):
        m = re.search("node ([0-9]*) free: ([0-9]*)[a-zA-A]*", line)
        if m:
            if int(m.group(2))>0:
                node_list.append(int(m.group(1)))
    return node_list[vnode_id % len(node_list)]
    

# obsolete
# this breaks when using pmem so we now construct it ourselves instead (see cpuset_of_socket)
def cpuset_of_socket_as_viewed_by_kernel(socket):
    p = subprocess.Popen(['numactl', '-H'], stdout=subprocess.PIPE, 
                                            stderr=subprocess.PIPE)

    out, err = p.communicate()
    for line in out.split('\n'):
        if re.search("node %d cpus" % (socket), line):
            cpulist=map(lambda x: int(x), line.split(':')[1].lstrip().split(' '))
            return cpulist

def cpuset_of_socket(socket_id, num_sockets, ncores_per_socket, nthreads_per_core):
    c = []
    num_cores = num_sockets * ncores_per_socket
    for n in range(nthreads_per_core):
        c.extend(range(num_cores*n+socket_id*ncores_per_socket, num_cores*n+(socket_id+1)*ncores_per_socket))
    return c


# distributes available cores equally among virtual nodes
def cpuset(vnode_id, total_vnodes, enable_ht, max_num_processors):
    nthreads_per_core = numthreadspercore()
    ncores_per_socket = numcorespersocket()
    nsockets = numsockets()
    ncores = nsockets * ncores_per_socket
    nthreads = ncores * nthreads_per_core

    if enable_ht:
        processors_per_vnode = nthreads / total_vnodes
        processors_per_socket = nthreads / nsockets
    else:
        processors_per_vnode = nthreads / total_vnodes / nthreads_per_core
        processors_per_socket = nthreads / nsockets / nthreads_per_core
    processors_per_vnode = min(processors_per_vnode, max_num_processors)
    if processors_per_vnode > processors_per_socket:
        numsockets_per_vnode = processors_per_vnode / processors_per_socket
    else:
        numsockets_per_vnode = 1
    cpusetl = []
    for i in range(numsockets_per_vnode):
        socket_id = vnode_id % (nsockets * numsockets_per_vnode) + i
        subset_id = vnode_id / (nsockets * numsockets_per_vnode) 
        cpulist = cpuset_of_socket(socket_id, nsockets, ncores_per_socket, nthreads_per_core)
        cpusetl.extend(cpulist[subset_id*processors_per_vnode:(subset_id+1)*processors_per_vnode])
    return map(lambda x: str(x), sorted(cpusetl))

def cpunum(vnode_id, total_vnodes, enable_ht, max_num_processors):
    return len(cpuset(vnode_id, total_vnodes, enable_ht, max_num_processors))

def cpuset_str(vnode_id, total_vnodes, enable_ht, max_num_processors):
    return ",".join(cpuset(vnode_id, total_vnodes, enable_ht, max_num_processors))
