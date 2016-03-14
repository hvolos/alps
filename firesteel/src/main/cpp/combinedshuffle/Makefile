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

GCC=/usr/bin/g++

##DEFINED_FLAGS   = -g -DDEBUG -O3 -fPIC
DEFINED_FLAGS   = -march=native -Wall  -fPIC -O3
##for sprof, turned on -g. Jun Li, 08/10/2015
##DEFINED_FLAGS   = -Wall -fPIC -g
##we will need the additional includes later
SHM_SRC_PATH = ../shuffle
SHM_SRC_EXTENSION_PATH = ../shuffle/longext
JNI_SRC_PATH = ../jnishuffle

RMB_INCLUDE = -I../../../../../../alps/src -I../../../../../../alps/third-party/yaml-cpp-0.5.2/include/
TBB_INCLUDE = -I/opt/concurrent/intel/tbb/include

CXXINC  = -I$(SHM_SRC_PATH) -I$(SHM_SRC_EXTENSION_PATH) $(RMB_INCLUDE) $(TBB_INCLUDE) -I$(JNI_SRC_PATH) -I/usr/local/include -I/usr/java/latest/include -I/usr/java/latest/include/linux
CPLUSPLUS11_OPTIONS=-std=c++11
CXXOPTS = $(CPLUSPLUS11_OPTIONS) $(DEFINED_FLAGS) $(CXXINC)
CXX     = $(GCC)

# for the linking to produce an executable 
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -ltcmalloc -lrt
RMB_SHAREDLIB_PATH  = ../../../../../../alps/build/src
RMB_SHAREDLIB  = alps
LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -L$(RMB_SHAREDLIB_PATH) -l$(RMB_SHAREDLIB) -ltcmalloc -L/opt/concurrent/intel/tbb/lib -ltbb -lnuma -lglog -lrt 
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -lglog -lrt 

SHARED_LIB_TO_BUILD =libjnishmshuffle.so
OTHERLIBS =  
CXXLIBS = $(LIB_TO_BUILD) $(OTHERLIBS)

##CFLAGS = $(CXXINC) -Ae -D_REENTRANT -g
SRCS	= ../shuffle/ExtensibleByteBuffers.cc \
	  ../shuffle/MapShuffleStoreManager.cc ../shuffle/MapShuffleStoreWithIntKeys.cc \
	  ../shuffle/MergeSortChannelHelper.cc   \
	  ../shuffle/MapShuffleStoreWithStringKeys.cc   \
	  ../shuffle/MergeSortReduceChannelWithIntKeys.cc  \
	  ../shuffle/MergeSortReduceChannelWithStringKeys.cc  \
	  ../shuffle/HashMapKeyPositionTracker.cc  \
	  ../shuffle/HashMapReduceChannelWithIntKeys.cc  \
          ../shuffle/PassThroughReduceChannelWithIntKeys.cc \
          ../shuffle/ReduceShuffleStoreManager.cc \
          ../shuffle/GenericReduceChannel.cc \
          ../shuffle/ReduceShuffleStoreWithIntKeys.cc  \
          ../shuffle/ReduceShuffleStoreWithStringKeys.cc  \
 	  ../shuffle/ShuffleDataSharedMemoryManagerHelper.cc \
          ../shuffle/ShuffleStoreManager.cc \
          ../shuffle/ByteBufferPool.cc \
          ../shuffle/ArrayBufferPool.cc \
          ../shuffle/longext/MapShuffleStoreWithLongKeys.cc \
          ../shuffle/longext/ReduceShuffleStoreWithLongKeys.cc \
          ../shuffle/longext/MergeSortReduceChannelWithLongKeys.cc \
          ../shuffle/longext/HashMapReduceChannelWithLongKeys.cc \
          ../shuffle/longext/PassThroughReduceChannelWithLongKeys.cc \
          ../jnishuffle/shuffle_MapSHMShuffleStore.cc ../jnishuffle/shuffle_ReduceSHMShuffleStore.cc \
          ../jnishuffle/shuffle_ShuffleStoreManager.cc 

 
LIB_OBJS    = $(SRCS:.cc=.o)

.SUFFIXES: .cc

.cc.o :
#	$(CXX) $(CXXOPTS) -c $<
	$(CXX) $(CXXOPTS) -c $< -o $@ 


##NOTE: this needs to be the first rule that is actionable. if we
##want to have "make" to assume "make all"
all : $(SHARED_LIB_TO_BUILD)


$(SHARED_LIB_TO_BUILD) :  $(LIB_OBJS) $(C_LIB_OBJS)
	$(CXX) $(LDOPTS) -o $@ $(LIB_OBJS) $(RMB_LIB)

clean :
	-rm -f core $(SHM_SRC_PATH)/*.o $(SHM_SRC_EXTENSION_PATH)/*.o $(JNI_SRC_PATH)/*.o
	-rm -f $(SHARED_LIB_TO_BUILD)

clobber : clean
	-rm -f $(SHARED_LIB_TO_BUILD)
