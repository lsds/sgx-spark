package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.internal.Logging

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

class SgxShmIteratorProviderIdentifier[K,V](myPort:Long, offset: Long, size: Int) extends SgxIteratorProvIdentifier[(K,V)](myPort) {
  
	override def getIterator(context: String) = new SgxShmIteratorConsumer[K,V](this, offset, size)

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ", offset=" + offset + ", size=" + size + ")"
}