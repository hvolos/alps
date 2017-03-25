# Installing ALPS on Fabric-Attached Memory platforms

This guide provides instructions for building and installing ALPS on Linux for
The Machine (L4TM) on a system equipped with Fabric-Attached Memory (FAM).

This guide assumes the ALPS source code is located in directory $ALPS_SRC and
built into directory $ALPS_BUILD.

## Dependencies

* We assume Linux OS. Testing has been done on L4TM 4.3.0-3 running on the
[FAME](https://hlinux-web.us.rdlabs.hpecorp.net/dokuwiki/doku.php/l4tm:qemu_fabric_experience) emulator. 
* We assume modern C/C++ compilers in the build environment that must 
  support C/C++11. Testing has been done with gcc version 4.9.2 or later. 
* We use the CMake family of build tools. Testing has been done with cmake version 3.0.2 or later.
* boost library
* bash 4.3


## Configuring Build Environment

Before building ALPS, please install any necessary packages by running the
accompanied script:

```
$ cd $ALPS_SRC
$ ./install-dep
```

Please note that the above script requires 'sudo' privilege rights.

In addition, please build and install the following dependencies from source 
as no packages are available at the moment.

- Install libfam-atomic

  ```
  $ cd ~
  $ sudo apt-get install autoconf autoconf-archive libtool
  $ sudo apt-get --no-install-recommends install asciidoc xsltproc xmlto
  $ git clone https://github.com/FabricAttachedMemory/libfam-atomic.git
  $ cd libfam-atomic
  $ bash autogen.sh
  $ ./configure
  $ make
  $ sudo make install
  ```


## Compilation

It is recommended to build ALPS outside the source tree to avoid polluting the
tree with generated binaries.
So first create a build directory $ALPS_BUILD into which you will configure
and build ALPS:

```
$ mkdir $ALPS_BUILD
```

Prepare the ALPS build system.
* For Debug build type (default):

  ```
  $ cmake $ALPS_BUILD -DTARGET_ARCH_MEM=NV-NCC-FAM -DCMAKE_BUILD_TYPE=Debug
  ```

* For Release build type:

  ```
  $ cmake $ALPS_BUILD -DTARGET_ARCH_MEM=NV-NCC-FAM -DCMAKE_BUILD_TYPE=Release
  ```

Finally, build ALPS:

```
$ make
```

## Testing

CTest is the simplest way to run the tests. It is best used when one needs
to quickly run just the unit tests against a local LFS installation that
serves as the development platform.

```
$ cd $ALPS_BUILD
ctest -R lfs
```

## Configuring Runtime Environment

ALPS has many operating parameters, which you can configure before starting a
program using ALPS.
ALPS initializes its internals by loading configuration options in the
following order:
* Load options from a system-wide configuration file: /etc/default/alps.[yml|yaml],
* Load options from the file referenced by the value of the environment
variable $ALPS_CONF, and
* Load options through a user-defined configuration file or object (passed
  through the ALPS API)

Deploying and using ALPS on a platform based on the Librarian File System (LFS)
and Fabric-Attached Memory (FAM) requires a properly configured environment.
The environment is set up through the use of ALPS configuration files.
One can either use [Holodeck](http://github.hpe.com/labs/holodeck)
to automatically configure the environment or configure it manually.

To manually configure, set the following configuration options in
a per-node configuration file (preferably the system-wide configuration
file):

```
LfsOptions:
  node: {{ lfs_node }}
  node_count: {{ lfs_node_count }}
  book_size_bytes: {{ lfs_book_size_bytes }}
```

These options **must** match the ones used by the Librarian Server and
Client running on the respective node.
