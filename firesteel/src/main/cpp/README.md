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

The shuffle engine consists of the following sub-directories:
(1) shuffle, the C++ shuffle engine
(2) jnishuffle, which contains the JNI header and implementation to bridge
    between Scala/Java and C++ engine 
(3) combinedshuffle, which contains only the Makefile to combine shuffle
    and jnishuffle to form a single shared library 

The following dependencies package will need to be installed:
*tcmalloc
*glog
*intel tbb

Depdending on where the dependent libraries are installed, the Makfile in
shufflef and combinedshuffle will need to be modified accordingly.

In jnishuffle, a symbolic link called "/usr/java/latest" needs to be 
created to point to the JDK top directory (e.g., jdk1.7.0_51), in order to 
support the following header includes in the corresponding Makefile:

 -I/usr/java/latest/include -I/usr/java/latest/include/linux 

to build the complete shared library of the C++ shuffle engine, in cobminedshuffle,
issue:

./make clean
./make 

