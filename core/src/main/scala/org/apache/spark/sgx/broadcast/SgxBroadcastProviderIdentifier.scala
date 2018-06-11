package org.apache.spark.sgx.broadcast

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager

import org.apache.spark.internal.Logging

class SgxBroadcastProviderIdentifier(myPort: Long) extends Serializable with Logging {
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		SgxBroadcastEnclave.init(con)
		con
	}

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}