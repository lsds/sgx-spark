package org.apache.spark.sgx.broadcast.shm

import org.apache.spark.sgx.ClientHandle
import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.shm.ShmCommunicationManager

class SgxShmBroadcastProvider() extends SgxBroadcastProvider() {

	private val _com = ShmCommunicationManager.get().newShmCommunicator(false)

	ClientHandle.sendRecv[Boolean](new SgxShmBroadcastProviderIdentifier(_com.getMyPort))

	val com = _com.connect(_com.recvOne.asInstanceOf[Long])

	override def toString() = this.getClass.getSimpleName + "(com=" + com + ")"
}
