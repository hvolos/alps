/*
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hp.hpl.firesteel.shuffle;


import com.esotericsoftware.kryo.Kryo;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To hold the shuffle resources on each runtime Spark task, at the map side and also the receiver side.
 * The current resources include Kryo instance that has all of the registered classes, and the ByteBuffer 
 * allocated from the native memory.
 * 
 * Since the same thread in an executor will be used as a Map thread or a Reduce thread. The same thread
 * local resources (kryo instance, byte buffer) can be re-used in either a Map thread or a Reduce Thread.
 *
 */
public class ThreadLocalShuffleResourceHolder {
	
	private static final Logger LOG = 
			             LoggerFactory.getLogger(ThreadLocalShuffleResourceHolder.class.getName());
	
	public static class ShuffleResource {
		private Kryo kryo;
		private ByteBuffer buffer; 
		//to add to the logical thread in the executor.
		private int logicalThreadId; 
		
		/**
		 * two kinds of resources: kryo instance and byte buffer instance
		 * @param kryoInstance the kryo instance that holds all of the registered classes
		 * @param bufferInstance the nio bytebuffer instance that is created off-heap. 
		 * @param logical thread id, the logical thread id that is meaningful only in the 
		 */
		public ShuffleResource (Kryo kryoInstance, ByteBuffer bufferInstance, int logicalThreadId) {
			this.kryo = kryoInstance;
			this.buffer = bufferInstance; 
			this.logicalThreadId = logicalThreadId; 
		}
		
		public Kryo getKryoInstance() {
			return this.kryo; 
		}
		
		public ByteBuffer getByteBuffer() {
			return this.buffer; 
		}
		
		public int getLogicalThreadId() {
			return this.logicalThreadId; 
		}
	}
	
	
	private static final ThreadLocal<ShuffleResource> holder = new ThreadLocal<ShuffleResource>();
	
	/**
	 * the two resources will have to be created outside this class. 
	 * NOTE: logical thread id will be called from Shuffle Store Manager to get the unique atomic counter. 
	 */
	public void initilaze (Kryo kryoInstance, ByteBuffer bufferInstance, int logicalThreadId) {
		ShuffleResource resource = new ShuffleResource (kryoInstance, bufferInstance, logicalThreadId);
		holder.set(resource);
	}
	
	/**
	 * retrieve the locally stored resource in the thread. If the resource is null, then
	 * we will need to initialize the resource. 
	 */
	public ShuffleResource getResource() {
		return holder.get();
	}
	
}
