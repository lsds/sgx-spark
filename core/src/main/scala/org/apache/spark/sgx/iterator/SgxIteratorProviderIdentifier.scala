/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sgx.iterator

import org.apache.spark.Partition
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.storage.DiskBlockObjectWriter
import org.apache.spark.util.collection.WritablePartitionedIterator

abstract class SgxIteratorProvIdentifier[T](myPort: Long) extends SgxIteratorIdentifier[T] with Logging {
  
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		con
	}
}

class SgxIteratorProviderIdentifier[T](myPort: Long) extends SgxIteratorProvIdentifier[T](myPort) {

	override def getIterator(context: String) = new SgxIteratorConsumer[T](this, context)

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}

class SgxShmIteratorProviderIdentifier[K,V](myPort:Long, offset: Long, size: Int, theSplit: Partition, inputMetrics: InputMetrics, splitLength: Long, splitStart: Long, delimiter: Array[Byte]) extends SgxIteratorProvIdentifier[(K,V)](myPort) {
  
	override def getIterator(context: String) = new SgxShmIteratorConsumer[K,V](this, offset, size, theSplit, inputMetrics, splitLength, splitStart, delimiter)

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ", offset=" + offset + ", size=" + size + ")"
}

class SgxWritablePartitionedIteratorProviderIdentifier[K,V](myPort:Long, offset: Long, size: Int) extends WritablePartitionedIterator with Serializable {
 
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		con
	}
	
	def hasNext(): Boolean = throw new RuntimeException("Not implemented: hasNext()")
  
	def nextPartition(): Int = throw new RuntimeException("Not implemented: nextPartition()")

	def writeNext(writer: DiskBlockObjectWriter): Unit = throw new RuntimeException("Not implemented: writeNext()")

	def getIterator() = new SgxWritablePartitionedIteratorConsumer[K,V](this, offset, size)

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}