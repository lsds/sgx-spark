package org.apache.spark.sgx.iterator.shm

import org.apache.spark.sgx.SgxCommunicationInterface
import org.apache.spark.sgx.SgxFactory
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.shm.ShmCommunicationManager

class SgxShmIteratorProviderIdentifier(myPort: Long) extends SgxIteratorProviderIdentifier {
	def connect(): SgxCommunicationInterface = {
		val con = ShmCommunicationManager.get().newShmCommunicator(false)
		con.connect(myPort)
		con.sendOne(con.getMyPort)
		con
	}

	override def toString() = getClass.getSimpleName + "(myPort=" + myPort + ")"
}
