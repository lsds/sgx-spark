package org.apache.spark.sgx.iterator.shm

import java.net.ServerSocket

import org.apache.spark.sgx.SgxFactory
import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.shm.ShmCommunicationManager

class SgxShmIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean) extends SgxIteratorProvider[T](delegate, inEnclave) {

	val com = ShmCommunicationManager.get().newShmCommunicator(false)

	val identifier = new SgxShmIteratorProviderIdentifier(com.getMyPort)

	override def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])

	override def toString() = this.getClass.getSimpleName + "(com=" + com + ")"
}