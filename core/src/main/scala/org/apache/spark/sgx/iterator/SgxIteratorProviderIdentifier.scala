package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.internal.Logging

abstract class SgxIteratorProvIdentifier[T] extends SgxIteratorIdentifier[T] with Logging {
  
}

class SgxIteratorProviderIdentifier[T](myPort: Long) extends SgxIteratorProvIdentifier[T] {
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		con
	}

	override def getIterator(context: String) = new SgxIteratorConsumer[T](this, context)

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}

class SgxShmIteratorProviderIdentifier[T](offset: Long, size: Int) extends SgxIteratorProvIdentifier[T] {

  logDebug("Creating " + this)

	override def getIterator(context: String) = new SgxShmIteratorConsumer[T](offset, size)

	override def toString() = getClass.getSimpleName + "(offset=" + offset + ", size=" + size + ")"
}