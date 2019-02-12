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

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.mapred.{EncryptedRecordReader, RecordReader}

import org.apache.spark.{InterruptibleIterator, Partition}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.{Encrypt, Serialization, SgxCallable, SgxSettings}
import org.apache.spark.sgx.shm.{MallocedMappedDataBuffer, MappedDataBufferManager, RingBuffProducer, ShmCommunicationManager}
import org.apache.spark.storage.DiskBlockObjectWriter
import org.apache.spark.util.NextIterator
import org.apache.spark.util.collection.WritablePartitionedIterator

abstract class SgxIteratorProv[T] extends InterruptibleIterator[T](null, null) with SgxIterator[T] with Logging {
  def getIdentifier: SgxIteratorProvIdentifier[T]
  
  def writeNext(writer: DiskBlockObjectWriter): Unit = {
    logError("Method writeNext() must be implemented by subclass")
    throw new RuntimeException("Method writeNext() must be implemented by subclass")
  }
}

class SgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean) extends SgxIteratorProv[T] with SgxCallable[Unit] {

	private val com = ShmCommunicationManager.newShmCommunicator(false)

	private val identifier = new SgxIteratorProviderIdentifier[T](com.getMyPort)

	private def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])

	override final def hasNext: Boolean = delegate.hasNext
  
  logDebug("xxx creating " + this)
	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the message interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
  override def next(): T = {
		throw new UnsupportedOperationException("Access this special Iterator via messages.")
	}

	override def getIdentifier = identifier

	def call(): Unit = {
		val com = do_accept
		logDebug(this + " got connection: " + com)

		while (isRunning) {
			com.recvOne() match {
				case num: MsgIteratorReqNextN =>
					val q = new ArrayBuffer[T](num.num)
					if (delegate.isInstanceOf[NextIterator[T]] && delegate.hasNext) {
						for (i <- 0 to num.num - 1 if delegate.hasNext) {
							val n = delegate.next
							// Need to clone the object. Otherwise the same object will be sent multiple times.
							// It seems that this is due to optimizations in the implementation of class NextIterator.
							// If cloning is not possible, just add this one object and send it individually.
							// This should never be the case, as the object _must_ be sent to a consumer.
							q.insert(i, SerializationUtils.clone(n.asInstanceOf[Serializable]).asInstanceOf[T])
						}
					} else {
						for (i <- 0 to num.num - 1 if delegate.hasNext) {
							val n = q.insert(i, delegate.next)
						}
					}
					val qe = if (doEncrypt) Encrypt(q) else q
					com.sendOne(qe)

				case MsgIteratorReqClose =>
					stop
					com.close()

				case x: Any =>
					logDebug(this + ": Unknown input message provided.")
			}
		}
	}

	override def toString() = {
	  this.getClass.getSimpleName + "(identifier=" + identifier + ", com=" + com + ", delegate=" + delegate.getClass.getName + "(" + delegate + "))"
	}
}

class SgxShmIteratorProvider[K,V](delegate: NextIterator[(K,V)], recordReader: RecordReader[K,V], theSplit: Partition, inputMetrics: InputMetrics, splitLength: Long, splitStart: Long, delimiter: Array[Byte]) extends SgxIteratorProv[(K,V)] with SgxCallable[Unit] {
  
	private val com = ShmCommunicationManager.newShmCommunicator(false)

	private val identifier = new SgxShmIteratorProviderIdentifier[K,V](com.getMyPort,
        if (SgxSettings.USE_HDFS_ENCRYPTION) recordReader.asInstanceOf[EncryptedRecordReader].getBufferOffset()
				else recordReader.getLineReader.getBufferOffset(),
        if (SgxSettings.USE_HDFS_ENCRYPTION) recordReader.asInstanceOf[EncryptedRecordReader].getBufferSize()
				else recordReader.getLineReader.getBufferSize(),
        theSplit, inputMetrics, splitLength, splitStart, delimiter)

	private def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])

	override def getIdentifier = identifier
  
	logDebug("Creating " + this)
	
	def call(): Unit = {
		val com = do_accept
	  
		logDebug(this + " got connection: " + com)

		while (isRunning) {
			val ret = com.recvOne() match {
				case c: SgxShmIteratorConsumerClose =>
					stop
					delegate.closeIfNeeded()
					Unit // don't need to reply to this type of messages
				case f: SgxShmIteratorConsumerFillBufferMsg =>
					if (SgxSettings.USE_HDFS_ENCRYPTION) recordReader.asInstanceOf[EncryptedRecordReader].fillBuffer()
					else recordReader.getLineReader.fillBuffer(f.inDelimiter)
			}
			if (ret != Unit) {
				logDebug("call(): sendOne " + ret + ", " + ret.getClass() + ", " + ret.getClass().getSimpleName)
				com.sendOne(ret)
			}
		}
	}
	
	override def toString() = this.getClass.getSimpleName + "(identifier=" + identifier + ")"
}

class SgxPair[K,V](val key: K, val value: V) extends Serializable {}

class SgxPartition(val id: Int) extends Serializable {}

class SgxDone extends Serializable {}


class SgxWritablePartitionedIteratorProvider[K,V](@transient it: Iterator[Product2[Product2[Int,K],V]], offset: Long, size: Int) extends WritablePartitionedIterator with SgxCallable[Unit] with Logging {
  
	private val buffer = new MallocedMappedDataBuffer(MappedDataBufferManager.get().startAddress() + offset, size)
	val writer = new RingBuffProducer(buffer, Serialization.serializer)
  
	private val com = ShmCommunicationManager.newShmCommunicator(false)

	private val identifier = new SgxWritablePartitionedIteratorProviderIdentifier[K,V](com.getMyPort, offset, size)

	private def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])
  
	logDebug("Creating " + this)

	def getIdentifier = identifier
	
	private[this] var cur = if (it.hasNext) it.next() else null

	def writeNext(writer: DiskBlockObjectWriter): Unit = {
		writer.write(cur._1._2, cur._2)
		cur = if (it.hasNext) it.next() else null
	}

	def hasNext(): Boolean = cur != null

	def nextPartition(): Int = cur._1._1

	def fill() = {
		while (it.hasNext) {
			val partitionId = nextPartition()
			if (partitionId != -1) {
				// TODO PL: creating an object for just an int is overkill
				writer.writeAny(new SgxPartition(partitionId))
			}
			// TODO PL: batching
			while (hasNext && nextPartition() == partitionId) {
				writer.writeAny(new SgxPair(cur._1._2, cur._2))
				cur = if (it.hasNext) it.next() else null
			}
		}
		// TODO PL: creating an object for nothing is overkill
		writer.writeAny(new SgxDone)
	}
	
	def call(): Unit = {
		val com = do_accept
	  
		logDebug(this + " got connection: " + com)
	  
		fill()

		while (isRunning) {
			val ret = com.recvOne() match {
				case c: SgxShmIteratorConsumerClose =>
					stop
					MappedDataBufferManager.get.free(buffer)
			}
			if (ret != null) {
				com.sendOne(ret)
			}
		}
	}
	
	override def toString() = this.getClass.getSimpleName + "(identifier=" + identifier + ", buffer=" + buffer + ")"
}


