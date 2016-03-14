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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;
import org.junit.Ignore; 
import junit.framework.Assert;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import com.esotericsoftware.kryo.Kryo;
import com.hp.hpl.firesteel.shuffle.SortBasedReduceSHMShuffleStoreWithIntKeysTest.ApplicationTestClass;

import java.util.ArrayList; 


public class KryoserializerTest  extends TestCase {

 private static final Logger LOG = LoggerFactory.getLogger(KryoserializerTest.class.getName());
 
	 public static class ApplicationTestClass {
	      private  int  pagerank;
	      private String pageurl;
	      private  int avgDuration;
	      
	      public ApplicationTestClass() {
	         pagerank = 0;
	         pageurl = null;
	         avgDuration = 0; 
	      }
	      
	      public ApplicationTestClass( int pr, String pu, int avg) {
	    	  this.pagerank = pr;
	    	  this.pageurl = pu;
	    	  this.avgDuration = avg; 
	      }
	  
	      
	      public int getPageRank() {
	    	  return this.pagerank;
	      }
	      
	      public String getPageUrl(){ 
	    	  return this.pageurl;
	      }
	      
	      public int getAvgDuration() {
	    	  return this.avgDuration;
	      }
	      
	      @Override public boolean equals(Object other) {
	    	    boolean result = false;
	    	    if (other instanceof ApplicationTestClass) {
	    	        ApplicationTestClass that = (ApplicationTestClass) other;
	    	        result = (this.getPageRank() == that.getPageRank() && this.getPageUrl().equals (that.getPageUrl())
	    	        		            && this.getAvgDuration() == that.getAvgDuration());
	    	    }
	    	    return result;
	    	}
	      
	 }
	 
	 @Override
	 protected void setUp() throws Exception{ 
		  
		 super.setUp();
		 
	 }
	 
	 
	 /**
	  *to test Shuffle Store Manager's load library, init and then shutdown 
	  */
	 @Test
	 public void testSerializerAndDeserializerWithClassRegistration() {
		  LOG.info("this is the test for serializerAndDeserializerWithClassRegistrationTest");
		  
		  Kryo kryoSerializerSide= new Kryo();
		  kryoSerializerSide.register(ApplicationTestClass.class);
		  
		  //create a direct bytebuffer:
		  int bufferSize = 1*1024*1024; // 1M bytes
		  ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		  MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer serializer = 
				   new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (kryoSerializerSide, byteBuffer);
		  serializer.init();
		  
		  //create ten objects
		  int numberOfObjects = 10;
		  
		  ArrayList<ApplicationTestClass> objectList = new ArrayList<ApplicationTestClass> (); 
		  for (int i=0; i<numberOfObjects; i++) {
			  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
			  objectList.add(obj);
		  }
 
		  serializer.writeClass(ApplicationTestClass.class); 
		  for (ApplicationTestClass obj: objectList) {
			  serializer.writeObject(obj);
		  }
		  
		  //then hand this to the deserializer. 
		  serializer.flush();
		  ByteBuffer serializedResultHolder = serializer.getByteBuffer();

		  
		  Kryo kryoDeserializerSide = new Kryo();
		  kryoDeserializerSide.register(ApplicationTestClass.class);
		  ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer deserializer = 
				    new  ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer(kryoDeserializerSide, serializedResultHolder);
		  
		  deserializer.init(); 
		  
		  Class retrievedClass = deserializer.readClass(); 
		  
		  Assert.assertTrue(retrievedClass.equals(ApplicationTestClass.class)); 
		  
		  int count=0;
		  boolean hasNext=true;
		  ArrayList<ApplicationTestClass> retrievedObjectList = new ArrayList<ApplicationTestClass> ();
		  while (hasNext) {
			   try {
			     Object obj = deserializer.readObject();
			     if (obj instanceof ApplicationTestClass) {
			       retrievedObjectList.add( (ApplicationTestClass) obj);
			       count++; 
			     }
			   }
			   catch (Exception ex) {
				   //this is intentional, when all of the objects have been written.
				   //LOG.error("kryo deserializer fails...", ex);
				   LOG.info ("when execption (expected, due to the end of buffer) happens, current count is: " +  count); 
				   hasNext = false; 
			   }
		  }
		  
		  Assert.assertEquals(count, numberOfObjects);
		 
		  for (int i=0; i<numberOfObjects; i++) {
			  Assert.assertTrue(retrievedObjectList.get(i).equals(objectList.get(i)));
		  }
		  
		  deserializer.close();
		  serializer.close(); 
	  
	 }
	 
	 
	 //NOTE: class registration only produces more efficiency. class is not necessary to be registered, in order to
	 //get through the Kryo serializer/de-serializer 
	 @Test
	 public void testSerializerAndDeserializerWithoutClassRegistration() {
		  LOG.info("this is the test for serializerAndDeserializerWithoutClassRegistrationTest");
		 
		  Kryo kryoSerializerSide= new Kryo();
		  //kryoSerializerSide.register(ApplicationTestClass.class);
		  
		  //create a direct bytebuffer:
		  int bufferSize = 1*1024*1024; // 1M bytes
		  ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(bufferSize);
		  MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer serializer = 
				   new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (kryoSerializerSide, byteBuffer);
		  serializer.init();
		  
		  //create ten objects
		  int numberOfObjects = 10;
		  
		  ArrayList<ApplicationTestClass> objectList = new ArrayList<ApplicationTestClass> (); 
		  for (int i=0; i<numberOfObjects; i++) {
			  ApplicationTestClass obj = new ApplicationTestClass (i, "hello" +i,  i+1); 
			  objectList.add(obj);
		  }
 
		  serializer.writeClass(ApplicationTestClass.class); 
		  for (ApplicationTestClass obj: objectList) {
			  serializer.writeObject(obj);
		  }
		  
		  //then hand this to the deserializer. 
		  serializer.flush();
		  ByteBuffer serializedResultHolder = serializer.getByteBuffer();

		  
		  Kryo kryoDeserializerSide = new Kryo();
		  //kryoDeserializerSide.register(ApplicationTestClass.class);
		  ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer deserializer = 
				    new  ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer(kryoDeserializerSide, serializedResultHolder);
		  
		  deserializer.init(); 
		  
		  Class retrievedClass = deserializer.readClass(); 
		  
		  Assert.assertTrue(retrievedClass.equals(ApplicationTestClass.class)); 
		  
		  int count=0;
		  boolean hasNext=true;
		  ArrayList<ApplicationTestClass> retrievedObjectList = new ArrayList<ApplicationTestClass> ();
		  while (hasNext) {
			   try {
			     Object obj = deserializer.readObject();
			     if (obj instanceof ApplicationTestClass) {
			       retrievedObjectList.add( (ApplicationTestClass) obj);
			       count++; 
			     }
			   }
			   catch (Exception ex) {
				   //this is intentional, when all of the objects have been written.
				   //LOG.error("kryo deserializer fails...", ex);
				   LOG.info ("when execption (expected, due to the end of buffer) happens, current count is: " +  count); 
				   hasNext = false; 
			   }
		  }
		  
		  Assert.assertEquals(count, numberOfObjects);
		 
		  for (int i=0; i<numberOfObjects; i++) {
			  Assert.assertTrue(retrievedObjectList.get(i).equals(objectList.get(i)));
		  }
		  
		  deserializer.close();
		  serializer.close(); 
	  
	 }
	 
	 @Test
	 public void testSerializeDeserializeClassDefinitionWithClassRegistration() {
		  LOG.info("this is the test for serializeDeserializeClassDefinitionWithClassRegistrationTest");
		 
		  Kryo kryoSerializerSide= new Kryo();
		  kryoSerializerSide.register(ApplicationTestClass.class);
		  
		  //create a direct bytebuffer:
		  int bufferSize = 1*1024; // 1Kbytes
		  ByteBuffer byteBuffer1 =  ByteBuffer.allocateDirect(bufferSize);
		  MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer serializer = 
				   new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (kryoSerializerSide, byteBuffer1);
		  serializer.init();
		  
		  ApplicationTestClass obj = new ApplicationTestClass (0, "hello" +0,  1); 
		  serializer.writeClass(obj.getClass());

	      int length1 = serializer.getByteBuffer().position(); //to copy the byte array out.
	      //NOTE: fundamentally, the following method actually re-set the position to zero, and then make copy!
	      byte[] vvalueType = serializer.toBytes();

	      //at the end, close it
	      serializer.close();

	      //now pick up by the de-serializer
	      
	      {
	    	  Kryo reduceSideKryo=new Kryo();
	 		  reduceSideKryo.register(ApplicationTestClass.class);
	 		 
			   //retrieve the value type information and check whether they are the same.
			   byte[] valueTypeDefinitionInBytes = vvalueType;
			   int length2 = valueTypeDefinitionInBytes.length;
			   ByteBuffer byteBuffer2 = ByteBuffer.allocate(length2 );
			   LOG.info("retrieved value type definition length is: " + length2);
			   //put into the buffer first.
			   byteBuffer2.put(valueTypeDefinitionInBytes);
			   LOG.info("after putting data, current  buffer's limit is: " + byteBuffer2.limit());
			   LOG.info("after putting data, current  buffer's position is: " + byteBuffer2.position());
			   
			   ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer deserializer = 
					      new ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer(reduceSideKryo, byteBuffer2);
			   
			   deserializer.init();
			   LOG.info("after init, current deserializer buffer's limit is: " + deserializer.getByteBuffer().limit());
			   LOG.info("after init, current deserializer buffer's position is: " + deserializer.getByteBuffer().position());
			   
			   Class retrievedClass = deserializer.readClass();
			   deserializer.close();
			   Assert.assertTrue(retrievedClass.equals(ApplicationTestClass.class)); 
		  }
	 }
	 
	 @Test
	 public void testSerializeDeserializeClassDefinitionWithoutClassRegistration() {
		  LOG.info("this is the test for serializeDeserializeClassDefinitionWithoutClassRegistrationTest");
		  
		  Kryo kryoSerializerSide= new Kryo();
		  //kryoSerializerSide.register(ApplicationTestClass.class);
		  
		  //create a direct bytebuffer:
		  int bufferSize = 1*1024; // 1Kbytes
		  ByteBuffer byteBuffer1 =  ByteBuffer.allocateDirect(bufferSize);
		  MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer serializer = 
				   new MapSHMShuffleStore.LocalKryoByteBufferBasedSerializer (kryoSerializerSide, byteBuffer1);
		  serializer.init();
		  
		  ApplicationTestClass obj = new ApplicationTestClass (0, "hello" +0,  1); 
		  serializer.writeClass(obj.getClass());

	      int length1 = serializer.getByteBuffer().position(); //to copy the byte array out.
	      //NOTE: fundamentally, the following method actually re-set the position to zero, and then make copy!
	      byte[] vvalueType = serializer.toBytes();

	      //at the end, close it
	      serializer.close();

	      //now pick up by the de-serializer
	      {
	    	   Kryo reduceSideKryo=new Kryo();
	 		  //reduceSideKryo.register(ApplicationTestClass.class);
	 		 
			   //retrieve the value type information and check whether they are the same.
			   byte[] valueTypeDefinitionInBytes = vvalueType;
			   int length2 = valueTypeDefinitionInBytes.length;
			   ByteBuffer byteBuffer2 = ByteBuffer.allocate(length2 );
			   LOG.info("retrieved value type definition length is: " + length2);
			   //put into the buffer first.
			   byteBuffer2.put(valueTypeDefinitionInBytes);
			   LOG.info("after putting data, current  buffer's limit is: " + byteBuffer2.limit());
			   LOG.info("after putting data, current  buffer's position is: " + byteBuffer2.position());
			   
			   ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer deserializer = 
					      new ReduceSHMShuffleStore.LocalKryoByteBufferBasedDeserializer(reduceSideKryo, byteBuffer2);
			   
			   deserializer.init();
			   LOG.info("after init, current deserializer buffer's limit is: " + deserializer.getByteBuffer().limit());
			   LOG.info("after init, current deserializer buffer's position is: " + deserializer.getByteBuffer().position());
			   
			   Class retrievedClass = deserializer.readClass();
			   deserializer.close();
			   Assert.assertTrue(retrievedClass.equals(ApplicationTestClass.class)); 
		  }
		 
	 }
	 
	 
	 @Override
	 protected void tearDown() throws Exception{ 
		 //do something first;
		 super.tearDown();
	 }
	 
	 public static void main(String[] args) {
		  
	      junit.textui.TestRunner.run(KryoserializerTest.class);
	 }
	 

}
