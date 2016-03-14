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

FIRESTEEL_ROOT="`dirname "$0"`"
FIRESTEEL_ROOT="`cd "$FIRESTEEL_ROOT"; pwd`"

export CC=/usr/bin/gcc
export CXX=/usr/bin/g++

# build dependencies
#GTEST_ROOT="`cd "$FIRESTEEL_ROOT/../external/gtest"; pwd`"
#$GTEST_ROOT/build.sh

# build rmb                                                                                             
#RMB_BUILD_DIR=${FIRESTEEL_ROOT}/build

#if [ ! -d ${RMB_BUILD_DIR} ]; then
#    mkdir ${RMB_BUILD_DIR}
#fi
#cd ${RMB_BUILD_DIR}
#cmake ${FIRESTEEL_ROOT}
#make

# build shm-shuffle and jni combined shared library
BUILD_DIR_SHM_SHUFFLE=${FIRESTEEL_ROOT}/src/main/cpp/combinedshuffle
BUILD_DIR_EDGE_PARTITION=${FIRESTEEL_ROOT}/src/main/cpp/jniedgepartition
BUILD_DIR_OFFHEAPSTORE=${FIRESTEEL_ROOT}/src/main/cpp/combinedoffheapstore

##if [ ! -d ${BUILD_DIR} ]; then
##    mkdir ${BUILD_DIR}
##fi

echo "build shmshuffle shared library at ${BUILD_DIR_SHM_SHUFFLE}"
cd ${BUILD_DIR_SHM_SHUFFLE}
make clean
make
ret=$? 
if [ $ret -eq 0 ]; then
    cp ${BUILD_DIR_SHM_SHUFFLE}/libjnishmshuffle.so $FIRESTEEL_ROOT/target/classes
fi

echo "build edgepartition shared library at ${BUILD_DIR_EDGE_PARTITION}"
cd ${BUILD_DIR_EDGE_PARTITION}
make clean
make
ret=$?
if [ $ret -eq 0 ]; then
    cp ${BUILD_DIR_EDGE_PARTITION}/libjniedgepartition.so $FIRESTEEL_ROOT/target/classes
fi

echo "build offheapstore shared library at ${BUILD_DIR_OFFHEAPSTORE}"
cd ${BUILD_DIR_OFFHEAPSTORE}
make clean
make
ret=$?
if [ $ret -eq 0 ]; then
        cp ${BUILD_DIR_OFFHEAPSTORE}/libjnioffheapstore.so $FIRESTEEL_ROOT/target/classes
fi

exit ${ret}
