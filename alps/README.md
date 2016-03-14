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

# ALPINiSM

ALPINiSM (Abstraction Layer for Programming Persistent Shared Memory)
provides a low-level abstraction layer that reliefs the user from the 
details of mapping, addressing, and allocating persistent shared memory 
(also known as fabric-attached memory).
This layer can be used as a building block for building higher level 
abstractions and data structures such as heaps, logs, etc.

## Building ALPINiSM

	$ cd $ALPS
	$ mkdir build
	$ cd build

For Debug build type (default):

	$ cmake .. -DTARGET_ARCH_MEM=NV-NCC-FAM -DCMAKE_BUILD_TYPE=Debug

For Release build type:

	$ cmake .. -DTARGET_ARCH_MEM=NV-NCC-FAM -DCMAKE_BUILD_TYPE=Release

## Primary Third-Party Dependencies

The library depends on the packages listed below. Internal development builds
these packages from source placed under a third-party directory $alps/third-party.
Alternatively, you can try installing the development packages available through
your Linux distribution.

gflags
gtest
libbacktrace
yaml-cpp-0.5.2


## Other Dependencies

apt-get install vim git
apt-get install cmake
apt-get install libnuma 
apt-get install libnuma-dev

apt-get install libconfig++-dev

sudo apt-get install libboost-all-dev

OR

sudo apt-get install libboost-dev
sudo apt-get install libboost-serialization-dev

sudo apt-get install libattr1-dev
sudo apt-get install libyaml-cpp-dev

sudo apt-get install python3

## Example Programs

ALPINiSM comes with several samples in the `examples` directory.

## Running Tests

## Style Guide 

We follow the Google C++ style guide available here:

https://google.github.io/styleguide/cppguide.html
