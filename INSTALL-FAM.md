# Installing ALPS on Fabric-Attached Memory platforms

This guide provides instructions for building and installing ALPS on Linux for
The Machine (L4TM) on a system equipped with Fabric-Attached Memory (FAM).

This guide assumes the ALPS source code is located in directory $ALPS_SRC and
built into directory $ALPS_BUILD.

## Dependencies

* We assume Linux OS. Testing has been done on L4TM 4.3.0-3 running on the
[FAME](https://hlinux-web.us.rdlabs.hpecorp.net/dokuwiki/doku.php/l4tm:qemu_fabric_experience) emulator. To get access to a machine running L4TM such as build-l4tm-X.u.labs.hpecorp.net, 
please email Robert Chapman (<robert.chapman@hpe.com>).
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

Once ALPS is built, unit tests can be run using CTest or Holodeck Mint.

### CTest

CTest is the simplest way to run the tests. It is best used when one needs
to quickly run just the unit tests against a local LFS installation that
serves as the development platform.

```
$ cd $ALPS_BUILD
ctest -R lfs
```

### Holodeck Mint

Holodeck is recommended over CTest when one needs to run all available tests,
including unit and integration tests, against an LFS installation running on
The Machine (emulated through FAME or TMAS).

To deploy ALPS and run tests:

```
cd $ALPS_SRC/scripts/deploy
./alps -c lfs.yaml install
./alps -c lfs.yaml unit     # run unit tests
./alps -c lfs.yaml test     # run integration tests
```

The script requires a Librarian installation file named *lfs.yaml* in the
working directory. This configuration file describes an existing Librarian
installation. ALPS ships with such a configuration file that you can use
and adapt to your setup.
Please refer to [Holodeck's Librarian documentation](https://github.hpe.com/labs/holodeck/blob/master/doc/librarian.md)
for instructions on how to setup Librarian using Holodeck MICA.


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