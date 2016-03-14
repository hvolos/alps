
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

Scripts to run a sparkle worker per each NUMA socket node.

# Selecting a file system for use with shuffle. 
 
Before creating the cluster, set the <spark.shuffle.fs> variable in template/conf/spark-defaults.conf
to one of the following [none|native|docker].
- none for network-based shuffle
- native for local file-system with native runs
- docker for local file-system use docker-based runs

# Examples:

1. Deploying Spark on a virtual cluster

Suppose we wanted to create a virtual cluster comprising 4 nodes
and deploy spark jarballs found under /home/volos/workspace/sparkle

./cluster create --spark-bin=/home/volos/workspace/sparkle --nodes=4
./start-all.sh

/var/tmp/vnode-master/spark/bin/spark-shell --master spark://$(hostname):7077

./kill-all.sh
./cluster destroy --nodes=4

2. Creating a virtual cluster with an ext4 shared file system

truncate /tmp/pmem0.pseudo --size=256g
yes | mkfs.ext4 /tmp/pmem0.pseudo

./cluster create --spark-bin=/home/volos/workspace/sparkle --nodes=4 --nvmfs-type=ext4 --pmem-dev=/tmp/pmem0.pseudo
