package org.apache.spark.sgx.broadcast.shm

import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.shm.ShmCommunicationManager

import scala.collection.mutable.HashMap

class SgxShmBroadcastProvider(broadcasts: HashMap[Long, Any]) extends SgxBroadcastProvider(broadcasts) {

	val com = ShmCommunicationManager.get().newShmCommunicator(false)

	com.sendOne(new SgxShmBroadcastProviderIdentifier(com.getMyPort))

	override def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])

	override def toString() = this.getClass.getSimpleName + "(com=" + com + ")"
}
