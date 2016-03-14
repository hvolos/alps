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

##DEFINED_FLAGS   = -g -DREAL_FLOAT -DDEBUG -O3 -fPIC
##DEFINED_FLAGS   = -w -fPIC -g -O3
##DEFINED_FLAGS   = -Wall -Wextra -fPIC -g -O3
DEFINED_FLAGS   = -march=native -Wall -fPIC -O3
##DEFINED_FLAGS   = -g -Wall -fPIC -O3
##we will need the additional includes later. 
##added for tcmalloc, June 10, 2014.
RMB_INCLUDE = -I../../../../../../alps/src -I../../../../../../alps/third-party/yaml-cpp-0.5.2/include/

CXXINC  = -I/usr/local/include $(RMB_INCLUDE) -I. -I./longext 
CPLUSPLUS11_OPTIONS=-std=c++11
CXXOPTS = $(CPLUSPLUS11_OPTIONS) $(DEFINED_FLAGS) $(CXXINC)
CXX     = $(GCC)

# for the linking to produce an executable 
##LDOPTS  = -Wall,-w -L$(BOOCHLIB)
##LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -ltbb -lrt -lglog -ltcmalloc
LDOPTS  = -shared -Wl,-soname,$(SHARED_LIB_TO_BUILD) -lrt -lglog

SHARED_LIB_TO_BUILD =liboffheapstore.so
OTHERLIBS = -lm 
CXXLIBS = $(LIB_TO_BUILD) $(OTHERLIBS)

##CFLAGS = $(CXXINC) -Ae -D_REENTRANT -g
SRCS	= OffHeapStoreManager.cc AttributeHashTable.cc 
           

LIB_OBJS    = $(SRCS:.cc=.o)

.SUFFIXES: .cc

.cc.o :
##	$(CXX) $(CXXOPTS) -c $<
	$(CXX) $(CXXOPTS) -c $< -o $@

##NOTE: this needs to be the first rule that is actionable. if we
##want to have "make" to assume "make all"
all : $(SHARED_LIB_TO_BUILD)


$(SHARED_LIB_TO_BUILD) :  $(LIB_OBJS)
	$(CXX) $(LDOPTS) -o $@ $(LIB_OBJS)

clean :
	-rm -f core *.o longext/*.o
	-rm -f $(SHARED_LIB_TO_BUILD)

clobber : clean
	-rm -f $(SHARED_LIB_TO_BUILD)
