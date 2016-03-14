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
GTEST_ROOT="`cd "$FIRESTEEL_ROOT/../external/gtest"; pwd`"
$GTEST_ROOT/build.sh

# build firesteel
BUILD_DIR=${FIRESTEEL_ROOT}/build

if [ ! -d ${BUILD_DIR} ]; then
    mkdir ${BUILD_DIR}
fi
cd ${BUILD_DIR}
cmake ${FIRESTEEL_ROOT}
make
ret=$? 
if [ $ret -eq 0 ]; then
    cp ${BUILD_DIR}/src/main/cpp/libfiresteel.so $FIRESTEEL_ROOT/target/classes
fi
exit ${ret}
