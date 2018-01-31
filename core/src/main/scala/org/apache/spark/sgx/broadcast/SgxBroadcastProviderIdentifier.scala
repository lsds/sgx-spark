package org.apache.spark.sgx.broadcast

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager

class SgxBroadcastProviderIdentifier(myPort: Long) extends Serializable {
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		SgxBroadcastEnclave.init(con)
		con
	}

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}