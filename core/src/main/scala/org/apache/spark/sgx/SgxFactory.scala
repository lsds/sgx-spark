package org.apache.spark.sgx

import java.net.ServerSocket
import java.net.Socket

import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.shm.SgxShmIteratorProvider
import org.apache.spark.sgx.iterator.socket.SgxSocketIteratorProvider
import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.sgx.sockets.SocketCommunicator
import org.apache.spark.sgx.sockets.SocketCommunicator

object SgxFactory {

	val server = new ServerSocket(SgxSettings.ENCLAVE_PORT)
	val mgr = ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_ENC_TO_OUT, SgxSettings.SHMEM_OUT_TO_ENC);
	Completor.submit(mgr);

	def newSgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean): SgxIteratorProvider[T] = {
		if (SgxSettings.SGX_USE_SHMEM) new SgxShmIteratorProvider[T](delegate, inEnclave);
		else new SgxSocketIteratorProvider[T](delegate, inEnclave);
	}

	def newSgxCommunicationInterface(): SgxCommunicator = {
		if (SgxSettings.SGX_USE_SHMEM) ShmCommunicationManager.get().newShmCommunicator()
		else {
			if (SgxSettings.IS_ENCLAVE) new SocketCommunicator(new Socket(SgxSettings.HOST_IP, SgxSettings.HOST_PORT))
			else new SocketCommunicator(new Socket(SgxSettings.ENCLAVE_IP, SgxSettings.ENCLAVE_PORT))
		}
	}

	def acceptCommunicator(): SgxCommunicator = {
		if (SgxSettings.SGX_USE_SHMEM) ShmCommunicationManager.get().accept()
		else new SocketCommunicator(server.accept())
	}
}