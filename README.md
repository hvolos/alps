[//]: # ( (c) Copyright 2016 Hewlett Packard Enterprise Development LP             )
[//]: # (                                                                          )
[//]: # ( Licensed under the Apache License, Version 2.0 (the "License");          )
[//]: # ( you may not use this file except in compliance with the License.         )
[//]: # ( You may obtain a copy of the License at                                  )
[//]: # (                                                                          )
[//]: # (     http://www.apache.org/licenses/LICENSE-2.0                           )
[//]: # (                                                                          )
[//]: # ( Unless required by applicable law or agreed to in writing, software      )
[//]: # ( distributed under the License is distributed on an "AS IS" BASIS,        )
[//]: # ( WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. )
[//]: # ( See the License for the specific language governing permissions and      )
[//]: # ( limitations under the License.                                           )


# Alps

![Alps](doc/figures/alps-logo.png)

Alps (Allocator for Persistent Shared memory) provides a low-level 
abstraction layer that reliefs the user from the details of mapping, 
addressing, and allocating persistent shared memory.
This layer can be used as a building block for building higher level 
abstractions and data structures such as heaps, logs, etc.

## Building Alps

```
$ cd $ALPS
$ mkdir build
$ cd build
```

### Building Alps for NUMA

For Debug build type (default):

```
$ cmake .. -DTARGET_ARCH_MEM=CC-NUMA -DCMAKE_BUILD_TYPE=Debug
$ make
```

For Release build type:

```
$ cmake .. -DTARGET_ARCH_MEM=CC-NUMA -DCMAKE_BUILD_TYPE=Release
$ make
```

## Installing Alps

To install in default location:

```
$ make install
```

To install in custom location: 

```
$ make DESTDIR=MY_CUSTOM_LOCATION install
```

## Installing Dependencies

To install necessary packages:

```
./install-dep
```

## Example Programs

Alps comes with several samples in the `examples` directory.


## Configuring Environment

Alps initializes its internals by loading configuration options 
in the following order: 
* Load options from a system-wide configuration file: /etc/default/alps.[yml|yaml],
* Load options from the file referenced by the value of the environment variable ALPS_CONF, and
* Load options through a user-defined configuration file or object (passed through the Alps API)

## Running Tests

Testing requires building Alps first. Once Alps, is built, 
unit tests can be run using CTest.

### TMPFS

To run unit tests against tmpfs (located at: /dev/shm):
```
ctest -R tmpfs
```

## Style Guide 

We follow the Google C++ style guide available here:

https://google.github.io/styleguide/cppguide.html
