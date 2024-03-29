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

import java.io.IOException
import java.util.concurrent.{Callable, Future, LinkedBlockingQueue}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.{EncryptedRecordReader, FileSplit, LineRecordReader, RecordReader}
import org.apache.hadoop.mapred.lib.CombineFileSplit

import org.apache.spark.Partition
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.sgx._
import org.apache.spark.sgx.shm.{MallocedMappedDataBuffer, MappedDataBufferManager, RingBuffConsumer}
import org.apache.spark.storage.DiskBlockObjectWriter
import org.apache.spark.util.NextIterator
import org.apache.spark.util.collection.WritablePartitionedIterator


class Filler[T](consumer: SgxIteratorConsumer[T]) extends Callable[Unit] with Logging {
  
	def call(): Unit = {
		val num = SgxSettings.PREFETCH
		if (num > 0) {
			val list = consumer.com.sendRecv[Encrypted](new MsgIteratorReqNextN(num)).decrypt[ArrayBuffer[T]]
			if (list.size == 0) {
				consumer.close
			}
			else consumer.objects.addAll({
        if (consumer.context == "" && list.size > 0 && list.head.isInstanceOf[Product2[Any,Any]] &&
          list.head.asInstanceOf[Product2[Any,Any]]._2.isInstanceOf[SgxFakePairIndicator]) {
					list.map(c => {
            val y = c.asInstanceOf[Product2[Encrypted,SgxFakePairIndicator]]._1.decrypt[Product2[Product2[Any,Any],Any]]
            (y._1._2,y._2).asInstanceOf[T]})
        }
        else list
      }.asJava)
		}
		logDebug("new objects: " + consumer.objects.size())
		consumer.Lock.synchronized {
			consumer.fillingFuture = null
		}
	}

	override def toString() = getClass.getSimpleName + "(consumer=" + consumer + ")"
}

class SgxIteratorConsumer[T](id: SgxIteratorProviderIdentifier[T], val context: String = "") extends Iterator[T] with Logging {

	logDebug(this + " connecting to: " + id)

	private var closed = false
	object Lock

	val com = id.connect()
	val objects = new LinkedBlockingQueue[T]()
	var fillingFuture : Future[Unit] = null
	fill()

	override def hasNext: Boolean = {
    logDebug("hasNext")
		fill()
		if (objects.size > 0) true
		else if (closed) false
		else {
			if (fillingFuture != null) fillingFuture.get
			objects.size > 0
		}
	}

	override def next: T = {
    logDebug("next")
		// This assumes that a next object exist. The caller needs to ensure
		// this by preceeding this call with a call to hasNext()
		val n = objects.take
		fill()
		n
	}

	def fill(): Unit = {
    logDebug(s"Fill ${objects.size}")
		if (objects.size <= SgxSettings.PREFETCH / 2) {
			Lock.synchronized {
				if (!closed && fillingFuture == null) {
					fillingFuture = Completor.submit(new Filler(this))
				}
			}
		}
	}

	def close() = {
		closed = true
		com.sendOne(MsgIteratorReqClose)
		com.close()
	}

	override def toString() = getClass.getSimpleName + "(id=" + id + ")"
}

class SgxShmIteratorConsumer[K,V](id: SgxShmIteratorProviderIdentifier[K,V], offset: Long, size: Int, theSplit: Partition, inputMetrics: InputMetrics, splitLength: Long, splitStart: Long, delimiter: Array[Byte]) extends NextIterator[(K,V)] with Logging {

  val buffer = new MallocedMappedDataBuffer(MappedDataBufferManager.get().startAddress() + offset, size)
  
  logDebug("Creating " + this)
  
  val com = id.connect()

  // com.sendRecv[Unit](new SgxShmIteratorConsumerClose())
  override def close() = com.sendOne(new SgxShmIteratorConsumerClose())
  
  /* Code from HadoopRDD follows */
  
  val split = theSplit.asInstanceOf[HadoopPartition]
  private val existingBytesRead = inputMetrics.bytesRead
  
  private val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
    case _: FileSplit | _: CombineFileSplit =>
      Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
    case _ => None
  }
  
  private def updateBytesRead(): Unit = {
    getBytesReadCallback.foreach { getBytesRead =>
      inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
    }
  }
  
  var reader: RecordReader[K, V] = if (SgxSettings.USE_HDFS_ENCRYPTION) new EncryptedRecordReader(buffer, delimiter, splitLength, splitStart, new SgxShmIteratorConsumerFillBuffer(com)).asInstanceOf[RecordReader[K,V]] else new LineRecordReader(buffer, delimiter, splitLength, splitStart, new SgxShmIteratorConsumerFillBuffer(com)).asInstanceOf[RecordReader[K,V]]
  
  private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
  private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

  override def getNext(): (K, V) = {
    try {
      finished = !reader.next(key, value)
    } catch {
      case e: IOException =>
        logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
        finished = true
    }
    if (!finished) {
      inputMetrics.incRecordsRead(1)
    } else {
      reader.close
      reader = null
    }
    if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
      updateBytesRead()
    }
    (key, value)
  }
	
  override def toString() = getClass.getSimpleName + "(offset=" + offset + ", size=" + size + ", buffer=" + buffer + ")"
}



class SgxWritablePartitionedIteratorConsumer[K,V](id: SgxWritablePartitionedIteratorProviderIdentifier[K,V], offset: Long, size: Int) extends WritablePartitionedIterator with Logging {
  
  private val buffer = new MallocedMappedDataBuffer(MappedDataBufferManager.get().startAddress() + offset, size)
  private val reader = new RingBuffConsumer(buffer, Serialization.serializer)
  private var partitionId = -1
  private var cur = null.asInstanceOf[SgxPair[K,V]]
  
  val com = id.connect()

  advanceToNext()
  
  logDebug("Creating " + this)

  def writeNext(writer: DiskBlockObjectWriter): Unit = {
    writer.write(cur.key, cur.value)
    if (!advanceToNext()) {
      if (SgxSettings.IS_ENCLAVE) com.sendOne(SgxShmIteratorConsumerClose)
      else MappedDataBufferManager.get().free(buffer)
    }
  }
  
  private def advanceToNext(): Boolean = {
    reader.readAny[Any]() match {
      case p: SgxPair[K,V] =>
        cur = p
        true
      case p: SgxPartition =>
        partitionId = p.id
        advanceToNext()
      case d: SgxDone =>
        cur = null
        false
    }
  }

  def hasNext(): Boolean = {
    cur != null
  }

  def nextPartition(): Int = partitionId

  override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}
