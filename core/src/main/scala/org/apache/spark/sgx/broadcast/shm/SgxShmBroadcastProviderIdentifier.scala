package org.apache.spark.sgx.broadcast.shm

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.broadcast.SgxBroadcastProviderIdentifier
import org.apache.spark.sgx.shm.ShmCommunicationManager

import scala.collection.mutable.HashMap

class SgxShmBroadcastProviderIdentifier(myPort: Long) extends SgxBroadcastProviderIdentifier {
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		con
	}

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}