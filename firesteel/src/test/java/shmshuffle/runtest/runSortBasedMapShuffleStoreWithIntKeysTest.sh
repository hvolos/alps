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

#!/bin/bash
# Script that executes test on shared-memory shuffle across Java, JNI and then C++ shuffle engine
 
# parameters of LSH Code
export LD_LIBRARY_PATH=/usr/local/lib:/opt/concurrent/intel/tbb/lib
##GLOG_V works!
export GLOG_v=3
##export GLOG_log_dir=/tmp/shmlog
java -Djava.library.path=/usr/local/lib -Xmx1024m -cp ../ShmShuffleEngine.jar:../extlibs/kryo-3.0.2.jar:../extlibs/reflectasm-1.10.1.jar:../extlibs/minlog-1.3.0.jar:../extlibs/objenesis-2.1.jar:../extlibs/asm-5.0.jar:../extlibs/slf4j-api-1.7.12.jar:../extlibs/slf4j-log4j12-1.7.12.jar:../extlibs/log4j-1.2.17.jar:../extlibs/junit-4.5.jar com.hp.hpl.firesteel.shuffle.SortBasedMapSHMShuffleStoreWithIntKeysTest




