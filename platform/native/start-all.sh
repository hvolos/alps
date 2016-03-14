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

if [ "$#" -ne 1 ]; then
    VCLUSTER_ROOT="/var/tmp"
    echo "setting default cluster path: ${VCLUSTER_ROOT}"
else
    VCLUSTER_ROOT=$1
fi

echo "starting master..."
${VCLUSTER_ROOT}/vnode-master/spark/sbin/start-master.sh

sleep 10s 

worker_id=0
while [ 1 ]
do
    WORKER_VNODE=${VCLUSTER_ROOT}/vnode-worker${worker_id}
    if [ -d ${WORKER_VNODE} ]; then
        echo "starting worker${worker_id}..."
        ${WORKER_VNODE}/spark/sbin/start-worker.sh
    else 
        break
    fi
    worker_id=$((worker_id + 1))
done
