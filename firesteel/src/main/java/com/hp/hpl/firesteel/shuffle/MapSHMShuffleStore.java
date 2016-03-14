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
//NOTE: this class only exists on Kryo.3.0.0, not the version that Spark 1.2.0 depends on
//(via Chill 0.5.0)
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import java.util.Arrays;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * to implement the Mapside Shuffle Store. we will use the Kryo serializer to do internal
 * object serialization/de-serialization
 * 
 */
public class MapSHMShuffleStore implements MapShuffleStore {

    private static final Logger LOG = LoggerFactory.getLogger(MapSHMShuffleStore.class.getName());
    /**
     * NOTE this class can only work on Kryo 3.0.0
     * to allow testing in Spark shm-shuffle package. 
     */
    public static class LocalKryoByteBufferBasedSerializer {

        private ByteBufferOutput output = null;
        private Kryo kryo = null;
        //to hold the original buffer, as buffer might be resized with a different buffer object.
        private ByteBuffer originalBuffer = null;

        /**
         * the buffer needs to be the nio based buffer.
         * @param kryo
         * @param buffer if the buffer can not hold the actual object(s) to be serialized. the
         *               buffer will grow using the nio buffer direct allocation. So when asking
         *               the position, we will need to use getByteBuffer to query the current position.
         */
        public LocalKryoByteBufferBasedSerializer (Kryo kryo, ByteBuffer buffer) {
            this.output= new ByteBufferOutput(buffer);
            this.kryo = kryo;
            this.originalBuffer=buffer;

        }

        public void writeObject(Object obj) {
            this.kryo.writeClassAndObject(this.output, obj);
        }

        public void writeClass(Class type) {
            this.kryo.writeClass(this.output, type);
        }


        /**
         * due to possible byte buffer resizing, we will have to use this API to accomodate possible
         * re-sizing of the byte buffer. this is per map task byte buffer, so we will not re-sizing again.
         *
         * @return
         */
        public ByteBuffer getByteBuffer(){
            return this.output.getByteBuffer();
        }

        /**
         * to copy the whole internal byte-buffer out.
         * @return the byte array containing the entire byte buffer's content.
         */
        public byte[] toBytes() {
        	return this.output.toBytes();
        }
        
        /**
         * position the bytebuffer to position=0
         */
        public void init() {
            //to avoid that the buffer gets expanded last time and then gets released at the time
            //of last marshalling.
        	this.originalBuffer.rewind();
            this.output.setBuffer(this.originalBuffer);
            //The flip() method switches a Buffer from writing mode to reading mode. 
            //Calling flip() sets the position back to 0, and sets the limit to where position just was.
            //details: http://tutorials.jenkov.com/java-nio/buffers.html#capacity-position-limit. 
            //this.output.getByteBuffer().flip();
        }
        
        /**
         * NOTE: as long as we do not pass an output stream to this serializer, this method does
         * not do anything. Thus, in our current implementation, this method call does not do anything.
         */
        public void flush() {
            this.output.flush();
        }

        /**
         * As long as we do not pass an output stream to this serializer, this method does
         * not do anything.
         */
        public void close() {
            this.output.close();
            //if I call release, this is no action taken regarding byte buffer;
            //if I call release, then the pass-in bytebuffer will be release.
            //what I need is to check whether my byte buffer really gets expanded or not
            //and I only release the one that is created due to expansion.
            //NOTE: only bytebuffer is not null. the default buffer is actually null. 
            ByteBuffer newBuffer = this.output.getByteBuffer();
            //reference comparision
            if (newBuffer != this.originalBuffer) {
                //the passed-in buffer gets expanded. then we release the expanded/
            	LOG.warn("SHM Shuffle serializer bytebuffe automatically expanded. Bigger serializer buffer reguired!");
                this.output.release();
            }
            this.output.close();
        }
    }



    private Kryo kryo= null;
    private ByteBuffer byteBuffer = null;
    private LocalKryoByteBufferBasedSerializer serializer = null;

    private ShuffleStoreManager shuffleStoreManager=null;

    private static AtomicInteger storeCounter = new AtomicInteger(0);
    private int storeId;  
    //serializer creates serialization instance. for a long lived executor, the thread pool can
    //reuse per-thread serialization instance

    /**
     * To have the caller to pass the serializer instance, which is Kryo Serializer.
     * @param kryo the instance that needs to be created from the caller. need to re-usable from a thread pool
     * @param byteBuffer needs to have the reusable bytebuffer from a  re-usable pool
     * as well
     */
    public  MapSHMShuffleStore(Kryo kryo, ByteBuffer byteBuffer,ShuffleStoreManager shuffleStoreManager) {
        this.kryo = kryo;
        this.byteBuffer = byteBuffer;
        this.serializer = new LocalKryoByteBufferBasedSerializer (kryo, byteBuffer);
        this.shuffleStoreManager= shuffleStoreManager;
        this.storeId = storeCounter.getAndIncrement(); 
    }

    private long pointerToStore=0;
    private int shuffleId=0;
    private int mapTaskId = 0;
    private int numberOfPartitions =0;
    private ShuffleDataModel.KValueTypeId keyType;
    
    //record the size of the pre-defined batch serialization size
    private int sizeOfBatchSerialization=0; 

    //the following two sets of array declaration is such that in one map shuffle whole duration,
    //we can pre-allocate the data structure required once, and then keep re-use for each iteration of 
    //store key/value pairs to C++ shuffle engine
    private int koffsets[] = null; 
    private int voffsets[]  =null; 
    private int npartitions[] = null; 
    
    //the following targets different key/value pair, not all of the defined structures will be activated
    //as it depends on which key type the map store is to handle. 
    private int nkvalues[] = null; 
    private float fkvalues[] = null; 
    private long  lkvalues[]  = null; 
    private String skvalues[] = null; 
    //for string length
    private int slkvalues[]=null; 
    
    //add key ordering specification for the map/reduce shuffle store
    private boolean ordering; 
    
    /**
     * to initialize the storage space for a particular shuffle stage's map instance
     * @param shuffleId the current stage id
     * @param mapTaskId  the current map task Id
     * @param numberOfPartitions  the total number of the partitions chosen for the reducer.
     * @param keyType the type of the key, so that we can create the corresponding one in C++.
     * @param batchSerialization, the size of the predefined serialization batch 
     * @param odering, to specify whether the keys need to be ordered or not, for the map shuffle.
     * @return sessionId, which is the pointer to the native C++ shuffle store.
     */
     @Override
     public void initialize (int shuffleId, int mapTaskId, int numberOfPartitions,
                                 ShuffleDataModel.KValueTypeId keyType, int batchSerialization,
                                 boolean ordering){

         LOG.info( "store id" + this.storeId + " map-side shared-memory based shuffle store started"  
                   + " with ordering: " + ordering);
         
         this.shuffleId = shuffleId;
         this.mapTaskId= mapTaskId;
         this.numberOfPartitions = numberOfPartitions;
         this.keyType = keyType;
         this.sizeOfBatchSerialization =  batchSerialization; 
         this.ordering = ordering;
         
         this.koffsets = new int[this.sizeOfBatchSerialization];
         this.voffsets = new int[this.sizeOfBatchSerialization];
         this.npartitions = new int[this.sizeOfBatchSerialization];
         
         //for different key types 
         if (keyType == ShuffleDataModel.KValueTypeId.Int) {
             this.nkvalues = new int[this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Float) {
            this.fkvalues = new float [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Long) {
             this.lkvalues = new long [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.String) {
             this.skvalues = new String[this.sizeOfBatchSerialization];
             this.slkvalues = new int [this.sizeOfBatchSerialization];
         }
         //for object, it has been take care of by store kv pairs and thus use koffsets and voffsets
         
         this.pointerToStore=
                 ninitialize(this.shuffleStoreManager.getPointer(),
                         shuffleId, mapTaskId, numberOfPartitions, keyType.getState(),
                         ordering);
     }

     private native long ninitialize(
             long ptrToShuffleManager, int shuffleId, int mapTaskId, int numberOfPartitions,
             int keyType, boolean ordering);


    @Override
    public void stop() {
        LOG.info( "store id " + this.storeId + " map-side shared-memory based shuffle store stopped with id:"
                + this.shuffleId + "-" + this.mapTaskId);
        nstop(this.pointerToStore);
    }

    //to stop and reclaim the DRAM resource.
    private native void nstop (long ptrToStore);

    /**
     * to shutdown the session and reclaim the NVM resources required for shuffling this map task.
     *
     * NOTE: who will issue this shutdown at what time
     */
    @Override
    public void shutdown() {
        LOG.info( "store id " + this.storeId + " map-side shared-memory based shuffle store shutdown with id:"
                    + this.shuffleId + "-" + this.mapTaskId);
        nshutdown(this.pointerToStore);
    }

    private native void nshutdown(long ptrToStore);
    /**
     * to serialize the (K,V) pairs  into the byte buffer, and record the offests in the byte
     * buffer for each K values and V values. Both K and V will be serialized. Note  we will
     * have to pool the ByteBuffer from the caller.
     *
     * @param kvalues the array of the K values
     * @param vvalues the array of the V values
     * @param numberOfPairs the size of the array of (K,V) pairs
     * 
     * the result about koffsets is in the array  the records each offset of the K values serialized in the
     *                  byte buffer
     * the result about voffsets is in the array that records each offset of the V values serialized in the
     *                 byte byffer
     */
    @Override
    public void serializeKVPairs (ArrayList<Object> kvalues, ArrayList<Object> vvalues, int numberOfPairs) {

        //this involves memory allocation. So we will have to pool it.
        this.serializer.init();
        for (int i=0; i<numberOfPairs;i++) {
            this.serializer.writeObject(kvalues.get(i));
            ByteBuffer currentKBuf = this.serializer.getByteBuffer();
            this.koffsets[i]= currentKBuf.position();
            this.serializer.writeObject(vvalues.get(i));
            ByteBuffer currentVBuf = this.serializer.getByteBuffer();
            this.voffsets[i]= currentVBuf.position();
        }

        //at the end, close it
        this.serializer.close();
    }

    /**
     * to store serialized (K,V) pairs stored in the byte buffer, into the C++ shared-memory
     * region
     * combined with the koffsets the offests for the K values
     * combined with the voffsets the offsets for the V values
     * @param partitions the partition number computed by the partitioner function for each
     *                  (K,V) pair
     * @param numberOfPairs the size of the (K,V) pairs that have been serialized and to be
     */
    @Override
    public void storeKVPairs(ArrayList<Integer> partitions, int numberOfPairs) {

    	if (!keyvalueTypeStored) {
    		throw new RuntimeException ( "store id " + this.storeId 
    				         + " both key type and value type should have been stored to C++ shuffle engine");
    	}
    	
        ByteBuffer holder=this.serializer.getByteBuffer(); //this may be re-sized.

        if (LOG.isDebugEnabled()) {
            byte[] bytesHolder = holder.array();
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {
                int kLength = 0;
                int kStart=0;
                int kEnd=0;
                if (i==0) {
                    kLength=this.koffsets[i];
                    kStart=0;
                    kEnd=this.koffsets[i]; //exclusive;
                }
                else {
                    kStart=voffsets[i-1];
                    kEnd=koffsets[i]; //exclusive
                    kLength=kEnd-kStart;
                }
                byte[] kValue = new byte[kLength];
                Arrays.copyOfRange(bytesHolder,kStart, kEnd);
                LOG.debug("store id " + this.storeId + " Key: " + i  + "with value: " + new String(kValue));


                int vStart=koffsets[i];
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                byte[] vValue = new byte[vLength];
                Arrays.copyOfRange(bytesHolder, vStart, vEnd);
                LOG.debug( "store id " + this.storeId + " value: " + i + " with value: " + new String(vValue));

                LOG.debug( "store id " + this.storeId + " partition Id: " + i + " is: "  + partitions.get(i));
            }

        }

        //only need the partition numbers to be passed in. 
        for (int i=0; i<numberOfPairs; i++) {
            this.npartitions[i] = partitions.get(i);
        }

        nstoreKVPairs(this.pointerToStore, holder, this.koffsets, this.voffsets, this.npartitions,numberOfPairs);
    }

    private native void nstoreKVPairs(long ptrToStore, ByteBuffer holder,
                                     int [] koffsets, int [] voffsets, int[] partitions, int numberOfPairs);

    /**
     * to serialize the V values for the(K,V) pairs that have the K values to be with type of
     * int, float, long, string that are supported by  C++.
     * @param vvalues the V values
     * the result is to store in private member voffsets: the array that records each offset of 
     * the V values serialized in the byte buffer
     * @param numberOfVs the size of the array of (K,V) pairs
     */
    @Override
    public void serializeVs (ArrayList<Object> vvalues, int numberOfVs) {
        //this involves memory allocation. So we will have to pool it.
        this.serializer.init();
        for (int i=0; i<numberOfVs;i++) {
            this.serializer.writeObject(vvalues.get(i));
            ByteBuffer currentVBuf = this.serializer.getByteBuffer();
            this.voffsets[i]= currentVBuf.position();
        }

        //at the end, close it
        this.serializer.close();
    }

    /**
     * Speical case: to store the (K,V) pairs that have the K values to be with type of Integer
     * combined with private member voffsets
     * @param kvalues
     * @param partitions
     * @param numberOfPairs
     */
    @Override
    public void storeKVPairsWithIntKeys (
                  ArrayList<Integer> kvalues, ArrayList<Integer> partitions, int numberOfPairs) {
    	//make sure that the value type has been stored.
    	if (!vvalueTypeStored) {
    		throw new RuntimeException ( "store id " + this.storeId + 
    				               " value type should have been stored to C++ shuffle engine");
    	}
    	
    	
        //NOTE: this byte buffer my be re-sized during serialization.
        ByteBuffer holder = this.serializer.getByteBuffer();
     
        if (LOG.isDebugEnabled()) 
        {
        	LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithIntKeys" + " numberOfPairs is: " + numberOfPairs);
        	//for direct buffer, array() throws unsupported operation exception. 
            //byte[] bytesHolder = holder.array();
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug ( "store id " + this.storeId + " " + i + "-th key's value: " + kvalues.get(i));

                int vStart=0;
                if (i>0) {
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                
                LOG.debug ("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug ("store id " + this.storeId + " [artition Id: " + i + " is: "  + partitions.get(i));
            }

        }

        //NOTE: can not use Array, as that would require Objects.
        for (int i=0; i<numberOfPairs; i++) {
            this.nkvalues[i] = kvalues.get(i);
            this.npartitions[i] = partitions.get(i);
        }

        nstoreKVPairsWithIntKeys(this.pointerToStore,
                                    holder, this.voffsets, this.nkvalues, npartitions, numberOfPairs);
    }

    private native void nstoreKVPairsWithIntKeys (long ptrToStore, ByteBuffer holder, int[] voffsets,
                                         int [] kvalues, int[] partitions, int numberOfPairs);
    /**
     * Speical case: to store the (K,V) pairs that have the K values to be with type of float
     * combined with private member voffsets
     * @param kvalues
     * @param partitions
     * @param numberOfPairs
     */
    @Override
    public void storeKVPairsWithFloatKeys (ArrayList<Float> kvalues, 
    		                                      ArrayList<Integer> partitions, int numberOfPairs) {
    	if (!vvalueTypeStored) {
    		throw new RuntimeException ( "store id " + this.storeId 
    				                          + " value type should have been stored to C++ shuffle engine");
    	}
    	
        ByteBuffer holder = this.serializer.getByteBuffer();
    
        if (LOG.isDebugEnabled()) {
            // parse the values and turn them into bytes.
        	LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithFloatKeys" + " numberOfPairs is: " + numberOfPairs);
        	
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + kvalues.get(i));

                int vStart=0;
                if (i>0){
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                
                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + partitions.get(i));
            }

        }

        for (int i=0; i<numberOfPairs; i++) {
            this.fkvalues[i] = kvalues.get(i);
            this.npartitions[i] = partitions.get(i);
        }

        nstoreKVPairsWithFloatKeys(this.pointerToStore, holder, this.voffsets, this.fkvalues, this.npartitions, numberOfPairs);
    }

    private native void nstoreKVPairsWithFloatKeys (long ptrToStrore, ByteBuffer holder, int[] voffsets,
                                           float[] kvalues, int[] partitions, int numberOfPairs);

    /**
     * Special case: to store the (K,V) pairs that have the K values to be with type of long
     * combined with private member: voffsets
     * @param kvalues
     * @param partitions
     * @param numberOfPairs
     */
    @Override
    public void storeKVPairsWithLongKeys (ArrayList<Long> kvalues, 
    		                                    ArrayList<Integer> partitions, int numberOfPairs) {

    	if (!vvalueTypeStored) {
    		throw new RuntimeException ("store id " + this.storeId + "value type should have been stored to C++ shuffle engine");
    	}
    	
        ByteBuffer holder = this.serializer.getByteBuffer();
       
        if (LOG.isDebugEnabled()) {
        	
        	LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithLongKeys" + " numberOfPairs is: " + numberOfPairs);
        	
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + kvalues.get(i));

                int vStart=0;
                if (i>0) {
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
               
                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + partitions.get(i));
            }

        }


        for (int i=0; i<numberOfPairs; i++) {
            this.lkvalues[i] = kvalues.get(i);
            this.npartitions[i] = partitions.get(i);
        }

        nstoreKVPairsWithLongKeys (this.pointerToStore, holder, this.voffsets, this.lkvalues, this.npartitions, numberOfPairs);
    }

    private native void nstoreKVPairsWithLongKeys (long ptrToStore, ByteBuffer holder, int[] voffsets,
                                          long[] kvalues, int[] partitions, int numberOfPairs);

    /**
     * Special case: To serialize the (K,V) pairs that have the K values to be with type of string
     * combined with private member: voffsets
     * @param kvalues
     * @param partitions
     * @param numberOfPairs
     */
    @Override
    public void storeKVPairsWithStringKeys (ArrayList<String> kvalues, 
    		                                    ArrayList<Integer> partitions, int numberOfPairs){
    	if (!vvalueTypeStored) {
    		throw new RuntimeException ( "store id" + this.storeId + "value type should have been stored to C++ shuffle engine");
    	}
    	
    	ByteBuffer holder = this.serializer.getByteBuffer();

        if (LOG.isDebugEnabled()) {
        	LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithStringKeys" + " numberOfPairs is: " + numberOfPairs);
        	
            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + kvalues.get(i));

                int vStart=0;
                if (i>0){
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                
                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + partitions.get(i));
            }

        }

        for (int i=0; i<numberOfPairs; i++) {
            this.skvalues[i] = kvalues.get(i);
            this.slkvalues[i]=this.skvalues[i].length();
            this.npartitions[i] = partitions.get(i);
        }

        nstoreKVPairsWithStringKeys (this.pointerToStore, holder, 
        		                     this.voffsets, this.skvalues, this.slkvalues, this.npartitions, numberOfPairs);

    }

    private native void nstoreKVPairsWithStringKeys (long ptrToStore, ByteBuffer holder, int[] voffsets,
                               String[] kvalues, int nkvalueLengths[], int partitions[], int numberOfPairs);

    /**
     * to sort and store the sorted data into non-volatile memory that is ready for  the reduder
     * to fetch
     * @return status information that represents the map processing status
     */
    @Override
    public ShuffleDataModel.MapStatus sortAndStore() {
         ShuffleDataModel.MapStatus status = new ShuffleDataModel.MapStatus ();

         //the status details will be updated in JNI.
         nsortAndStore(this.pointerToStore, this.numberOfPartitions, status);
         
         //show the information 
         long retrieved_mapStauts[] = status.getMapStatus();
         long retrieved_shmRegionIdOfIndexChunk = status.getRegionIdOfIndexBucket();
         long retrieved_offsetToIndexBucket = status.getOffsetOfIndexBucket();
         
         if (LOG.isDebugEnabled()) { 
	         LOG.debug ("store id " + this.storeId + 
	        		 " in sortAndStore, total number of buckets is: " + retrieved_mapStauts.length);
	         for (int i=0; i<retrieved_mapStauts.length; i++) {
	         	LOG.debug ("store id" + this.storeId + " **in sortAndStore " + i + "-th bucket size: " + retrieved_mapStauts[i]);
	         }
	         LOG.debug ("store id " + this.storeId + 
	        		 " **in sortAndStore, retrieved shm region name: " + retrieved_shmRegionIdOfIndexChunk);
	         LOG.debug ("store id " + this.storeId +
	        		 " **in sortAndStore, retrieved offset to index chunk is:" + retrieved_offsetToIndexBucket);
         }
         
         return status;
    }

    /**
     * The map status only returns for each map's bucket, what each reducer will get what size of
     * byte-oriented content.
     *
     * @param ptrToStoreMgr: pointer to store manager.
     * @param mapStatus the array that will be populated by the underly C++ shuffle engine, before
     *                  it gets returned.
     * @return the Map status information on all of the buckets produced from the Map task.
     */
    private native void nsortAndStore (long ptrToStore, 
                                       int totalNumberOfPartitions,  ShuffleDataModel.MapStatus mapStatus);

    /**
     * to query what is the K value type used in this shuffle store
     * @return the K value type used for this shuffle operation.
     */
    @Override
    public ShuffleDataModel.KValueTypeId getKValueTypeId() {
    	return (this.ktypeId);
    }

    private ShuffleDataModel.KValueTypeId ktypeId = ShuffleDataModel.KValueTypeId.Object;

    /**
     * to set the K value type used in this shuffle store
     * @param ktypeId
     */
    @Override
    public void setKValueTypeId(ShuffleDataModel.KValueTypeId ktypeId) {
        this.ktypeId=ktypeId;
    }


    private byte[] vvalueType=null;
    private byte[] kvalueType=null;

    //to record whether the value type has been pushed down to the C++ shuffle engine or not. 
    private boolean vvalueTypeStored = false; 
    /**
     * based on a given particular V value, to store its value type. The corresponding key is either Java 
     * primitive types (int, long, float, double), or String. 
     * 
     * @param Vvalue
     */
    @Override
    public void storeVValueType(Object Vvalue) {
    	if (!vvalueTypeStored) {
	        this.serializer.init();
	        this.serializer.writeClass(Vvalue.getClass());
	
	        this.vvalueType = this.serializer.toBytes();
	     
	        //at the end, close it
	        this.serializer.close();
	
	        if (LOG.isDebugEnabled()) {
	        	LOG.debug("store id " + this.storeId + " value Type size: " + this.vvalueType.length);
	        }
	        nstoreVValueType(this.pointerToStore, this.vvalueType, this.vvalueType.length);
	        //update the flag
	        vvalueTypeStored=true;
    	}
    }

    private native void nstoreVValueType(long ptrToStore, byte[] vvalueType, int vtypeLength);

    //to record whether the value type has been pushed down to the C++ shuffle engine or not. 
    private boolean keyvalueTypeStored = false; 
    
    /**
     * based on a given particular object based (K,V) pair, to store the corresponding types for
     * both Key and Value.
     * @param Kvalue
     * @param Vvalue
     */
    @Override
    public void storeKVTypes (Object Kvalue, Object Vvalue){
    	if (!keyvalueTypeStored) {
	        this.serializer.init();
	
	        //for Key value type
	        this.serializer.writeClass(Kvalue.getClass());
	        int kposition =  this.serializer.getByteBuffer().position();
	
	        int klength = this.serializer.getByteBuffer().position(); //to copy the byte array out.
	        this.kvalueType = new byte[klength];
	        this.serializer.getByteBuffer().put(this.vvalueType);
	
	        //for Value type
	         this.serializer.writeClass(Vvalue.getClass());
	         int vposition = this.serializer.getByteBuffer().position();
	
	         byte[] fullBuffer = new byte[vposition];
	         this.serializer.getByteBuffer().get(fullBuffer);
	
	         //copy only a portition of it to the vvalueType
	         this.vvalueType = Arrays.copyOfRange(fullBuffer, kposition, vposition);
	
	        //at the end, close it
	        this.serializer.close();
	
	        if (LOG.isDebugEnabled()) {
	            LOG.debug("store id " + this.storeId + " key Type:" + new String (this.kvalueType));
	            LOG.debug("store id " + this.storeId + " value Type: " + new String(this.vvalueType));
	        }
	
	        nstoreKVTypes(this.shuffleStoreManager.getPointer(), shuffleId, mapTaskId,
	        		            this.kvalueType, this.kvalueType.length,
	        		            this.vvalueType, this.vvalueType.length);
	        
	        keyvalueTypeStored=true;
    	}
    }

    private native void nstoreKVTypes(long ptrToStoreMgr,
                                      int shuffleId, int mapId, 
                                      byte[] kvalueType, int ktypeLength, byte[] vvalueType, int vtypeLength);

    /**
     * to support when K value type is an arbitrary object type. to retrieve the serialized
     * type information for K values that can be de-serialized by Java/Scala
     */
    @Override
    public byte[] getKValueType() {
        return this.kvalueType;
    }

    /**
     * to retrieve the serialized type information for the V values that can be
     * de-serialized by Java/Scala
     */
    @Override
    public  byte[] getVValueType() {
         return this.vvalueType;
    }

    
    @Override
    public int getStoreId() {
    	return this.storeId; 
    }
}
