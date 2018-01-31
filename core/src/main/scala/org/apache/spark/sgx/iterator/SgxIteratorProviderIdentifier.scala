package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.shm.ShmCommunicationManager

class SgxIteratorProviderIdentifier(myPort: Long) extends Serializable {
	def connect(): SgxCommunicator = {
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		con
	}

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}