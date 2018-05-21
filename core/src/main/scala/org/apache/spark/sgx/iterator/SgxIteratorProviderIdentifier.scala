package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.executor.InputMetrics
import org.apache.spark.Partition

abstract class SgxIteratorProvIdentifier[T](myPort: Long) extends SgxIteratorIdentifier[T] with Logging {
  
  logDebug("Creating " + this)
  
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
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