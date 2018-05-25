package org.apache.spark.sgx.iterator

import java.util.concurrent.{Executors, Callable, ExecutorCompletionService, Future}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import scala.collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.Completor
import org.apache.spark.sgx.Encrypted
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.shm.ShmCommunicationManager;
import org.apache.spark.sgx.shm.MappedDataBufferManager
import org.apache.spark.sgx.shm.MappedDataBuffer
import org.apache.spark.sgx.shm.MallocedMappedDataBuffer
import org.apache.spark.util.NextIterator
import org.apache.spark.deploy.SparkHadoopUtil
import java.io.IOException
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.Partition
import org.apache.spark.executor.InputMetrics
import java.text.SimpleDateFormat
import org.apache.spark.rdd.HadoopRDD
import java.util.Locale
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.LineRecordReader
import org.apache.spark.sgx.shm.RingBuffProducer
import org.apache.spark.sgx.shm.RingBuffConsumer
import org.apache.spark.sgx.Serialization
import org.apache.spark.sgx.shm.RingBuffProducer
import org.apache.spark.sgx.shm.RingBuffConsumer


class Filler[T](consumer: SgxIteratorConsumer[T]) extends Callable[Unit] with Logging {
	def call(): Unit = {
		val num = SgxSettings.PREFETCH //- consumer.objects.size
		if (num > 0) {
			val list = consumer.com.sendRecv[Encrypted](new MsgIteratorReqNextN(num)).decrypt[ArrayBuffer[T]]
			
			if (list.size == 0) {
				consumer.close
			}
			else consumer.objects.addAll({
        if (consumer.context == "" && list.size > 0 && list.head.isInstanceOf[Product2[Any,Any]] && list.head.asInstanceOf[Product2[Any,Any]]._2.isInstanceOf[SgxFakePairIndicator]) {				  
				  list.map(c => {
				    val y = c.asInstanceOf[Product2[Encrypted,SgxFakePairIndicator]]._1.decrypt[Product2[Product2[Any,Any],Any]]
				    (y._1._2,y._2).asInstanceOf[T]
				  })
				} else list
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
		fill()
		if (objects.size > 0) true
		else if (closed) false
		else {
			if (fillingFuture != null) fillingFuture.get
			objects.size > 0
		}
	}

	override def next: T = {
		// This assumes that a next object exist. The caller needs to ensure
		// this by preceeding this call with a call to hasNext()
		val n = objects.take
		fill()
		n
	}

	def fill(): Unit = {
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

class SgxShmIteratorConsumer[K,V](id: SgxShmIteratorProviderIdentifier[K,V], writerOff: Long, readerOff: Long, offset: Long, size: Int, size2: Int, theSplit: Partition, inputMetrics: InputMetrics, splitLength: Long, splitStart: Long, delimiter: Array[Byte]) extends NextIterator[(K,V)] with Logging {

  val buffer = new MallocedMappedDataBuffer(MappedDataBufferManager.get().startAddress() + offset, size)
  
  val readr = new RingBuffConsumer(new MallocedMappedDataBuffer(MappedDataBufferManager.get().startAddress() + readerOff, size2), Serialization.serializer);
  val writer = new RingBuffProducer(new MallocedMappedDataBuffer(MappedDataBufferManager.get().startAddress() + writerOff, size2), Serialization.serializer);  
  
  logDebug("Creating " + this)
  
  val com = id.connect()
  
  writer.write(1234);
  
  override def close(): Unit = 1//com.sendRecv[Unit](new SgxShmIteratorConsumerClose())
  
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
  
  var reader: RecordReader[K, V] = new LineRecordReader(buffer, delimiter, splitLength, splitStart, new SgxShmIteratorConsumerFillBuffer(com, readr, writer)).asInstanceOf[RecordReader[K,V]]
  
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
    }
    if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
      updateBytesRead()
    }
    (key, value)
  }
	
	override def toString() = getClass.getSimpleName + "(offset=" + offset + ", size=" + size + ", buffer=" + buffer + ")"
}
