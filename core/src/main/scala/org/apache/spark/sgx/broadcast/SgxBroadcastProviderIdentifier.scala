package org.apache.spark.sgx.broadcast

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager

import org.apache.spark.internal.Logging

class SgxBroadcastProviderIdentifier(myPort: Long) extends Serializable with Logging {
	def connect(): SgxCommunicator = {
	  logDebug("connect 1")
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
		logDebug("connect 2 + con")
		con.connect(myPort)
		logDebug("connect 3")
		con.sendOne(con.getMyPort)
		logDebug("connect 4")
		SgxBroadcastEnclave.init(con)
		logDebug("connect 5" + con)
		con
	}

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}