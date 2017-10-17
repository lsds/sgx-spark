package org.apache.spark.sgx

import java.net.Socket

import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.socket.SgxSocketIteratorProvider
import org.apache.spark.sgx.sockets.SocketHelper

object SgxFactory {
	def newSgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean): SgxIteratorProvider[T] = {
		return new SgxSocketIteratorProvider[T](delegate, inEnclave);
	}

	def newSgxCommunicationInterface(): SgxCommunicationInterface = {
		if (SgxSettings.SGX_USE_SHMEM) ShmCommunicationManager.get().newShmCommunicator()
		else {
			if (SgxSettings.IS_ENCLAVE) new SocketHelper(new Socket(SgxSettings.HOST_IP, SgxSettings.HOST_PORT))
			else new SocketHelper(new Socket(SgxSettings.ENCLAVE_IP, SgxSettings.ENCLAVE_PORT))
		}
	}
}