#!/bin/bash

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


# default values
HDFS_DATA_SIZE='128g' # Total size of the for HDFS data. This is equally split among per-datanode container volumes.
HDFS_NAME_VOLUME_SIZE='32g' # Size of the volume used for HDFS Namenode
SPARK_LOCAL_VOLUME_SIZE='32g' # Size of the per-container volume used for Spark intermediate files

# enable hyperthreading
ENABLE_HT=1

# volume name identifiers
HDFS_NAME_VOLUME_ID=0
HDFS_DATA_VOLUME_ID=1
SPARK_VOLUME_ID=2

# public API functions

function str_to_size() {
    local str=$1
    local __ret=$2
    size_char=${str:(-1)}
    if [[ $size_char -eq 'm' ]]; then
        mult="1024*1024"
    fi
    if [[ $size_char -eq 'g' ]]; then
        mult="1024*1024*1024"
    fi
    local sizes="${str:0:${#str}-1}"
    local ret=`echo "scale=0;${sizes} * ${mult}" | bc -l`
    eval $__ret="'${ret}'"
}

function vnode_to_socket() {
    local vnode_id=$1
    local __ret=$2
    local num_sockets=`numactl -H | grep available | awk '{print $2}'`
    local ret=`echo "scale=0;${vnode_id} % ${num_sockets}" | bc -l`
    eval $__ret="'${ret}'"
}

function vnode_volmap_hdfs_namenode() {
    local vnode_id=$1
    local __ret=$2
    local hdfs_vol_path=$(tmpfs_path ${vnode_id} ${HDFS_NAME_VOLUME_ID})
    echo 'HDFS namenode volume path: ' ${hdfs_vol_path}
    mount_tmpfs ${hdfs_vol_path} ${vnode_id} ${HDFS_NAME_VOLUME_ID} ${HDFS_NAME_VOLUME_SIZE}
    local ret="-v ${hdfs_vol_path}:/mnt/hdfs"
    eval $__ret="'$ret'"
}

function vnode_volume_create_all() {
    local num_vnodes=$1
    local nworkers_minus_one=`echo "scale=0;(${num_vnodes}-1)" | bc -l`
    local vnode_id
    for vnode_id in `seq 0 $nworkers_minus_one`; do
        local hdfs_vol_path=$(tmpfs_path ${vnode_id} ${HDFS_DATA_VOLUME_ID})
        local local_vol_path=$(tmpfs_path ${vnode_id} ${SPARK_VOLUME_ID})
        echo 'HDFS datanode volume path: ' ${hdfs_vol_path}
        echo 'SPARK local volume path: ' ${local_vol_path}
        local hdfs_size_bytes
        str_to_size $HDFS_DATA_SIZE hdfs_size_bytes
        hdfs_data_volume_size=`echo "scale=0;${hdfs_size_bytes} / ${num_vnodes}" | bc -l`
        if [ $num_vnodes -eq 1 ]; then
            mount_tmpfs_nobind ${hdfs_vol_path} ${vnode_id} ${HDFS_DATA_VOLUME_ID} ${hdfs_data_volume_size}
            mount_tmpfs_nobind ${local_vol_path} ${vnode_id} ${SPARK_VOLUME_ID} ${SPARK_LOCAL_VOLUME_SIZE}
        else
            mount_tmpfs ${hdfs_vol_path} ${vnode_id} ${HDFS_DATA_VOLUME_ID} ${hdfs_data_volume_size}
            mount_tmpfs ${local_vol_path} ${vnode_id} ${SPARK_VOLUME_ID} ${SPARK_LOCAL_VOLUME_SIZE}
        fi
    done
}

function vnode_volume_map_all() {
    local num_vnodes=$1
    local __ret=$2
    local nworkers_minus_one=`echo "scale=0;(${num_vnodes}-1)" | bc -l`
    local ret=""
    local vnode_id
    for vnode_id in `seq 0 $nworkers_minus_one`; do
        local hdfs_vol_path=$(tmpfs_path ${vnode_id} ${HDFS_DATA_VOLUME_ID})
        local local_vol_path=$(tmpfs_path ${vnode_id} ${SPARK_VOLUME_ID})
        local ret=$ret" -v ${local_vol_path}:/mnt/spark-worker"$vnode_id
    done
    eval $__ret="'$ret'"
}

function vnode_volume_map() {
    local vnode_id=$1
    local num_vnodes=$2
    local __ret=$3
    local hdfs_vol_path=$(tmpfs_path ${vnode_id} ${HDFS_DATA_VOLUME_ID})
    local local_vol_path=$(tmpfs_path ${vnode_id} ${SPARK_VOLUME_ID})
    local ret="-v ${hdfs_vol_path}:/mnt/hdfs -v ${local_vol_path}:/mnt/spark"
    local ret
    eval $__ret="'$ret'"
}

function execute_python() {
    local cmd=$1
    local __ret=$2
    local module_dir=$(cd $(dirname $0); pwd)
    local ___ret=`bash -c "cd $module_dir; python -c \"$cmd\""`
    eval $__ret="'$___ret'"
}

function vnode_cpuset() {
    local vnode_id=$1
    local num_vnodes=$2
    local max_num_processors=$3
    local __ret=$4
    local ret
    execute_python "import vnode; print vnode.cpuset_str(${vnode_id},${num_vnodes},${ENABLE_HT},${max_num_processors})" ret
    eval $__ret="'$ret'"
}

function vnode_cpunum() {
    local vnode_id=$1
    local num_vnodes=$2
    local max_num_processors=$3
    local __ret=$4
    local ret
    execute_python "import vnode; print vnode.cpunum(${vnode_id},${num_vnodes},${ENABLE_HT},${max_num_processors})" ret
    eval $__ret="'$ret'"
}



function destroy_all_vnodes() {
    # umount all tmpfs
    df | grep mnt | awk '{print $6}' | xargs umount
}

# private functions

function tmpfs_path() {
    local vnode_id=$1
    local volume_id=$2
    echo "/mnt/tmpfs-${vnode_id}_${volume_id}"
}

function mount_tmpfs() {
    local mntpath=$1
    local vnode_id=$2
    local vol_id=$3
    local size=$4
    vnode_to_socket $vnode_id socket_id
    mkdir -p $mntpath
    if [ "$DEBUG" -gt 0 ]; then
        echo "mount -t tmpfs -o noatime,size=${size},mpol=bind:${socket_id} tmpfs ${mntpath}"
    fi
    mount -t tmpfs -o noatime,size="${size}",mpol=bind:${socket_id} tmpfs ${mntpath}
}

function mount_tmpfs_nobind() {
    local mntpath=$1
    local vnode_id=$2
    local vol_id=$3
    local size=$4
    vnode_to_socket $vnode_id socket_id
    mkdir -p $mntpath
    if [ "$DEBUG" -gt 0 ]; then
        echo "mount -t tmpfs -o noatime,size=${size} tmpfs ${mntpath}"
    fi
    mount -t tmpfs -o noatime,size="${size}" tmpfs ${mntpath}
}
