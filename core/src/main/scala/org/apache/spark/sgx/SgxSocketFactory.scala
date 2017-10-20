package org.apache.spark.sgx

import java.net.ServerSocket
import java.net.Socket

import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.socket.SgxSocketIteratorProvider
import org.apache.spark.sgx.sockets.SocketCommunicator

object SgxSocketFactory extends SgxFactory {

	val server = new ServerSocket(SgxSettings.ENCLAVE_PORT)

	def newSgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean): SgxIteratorProvider[T] = {
		new SgxSocketIteratorProvider[T](delegate, inEnclave);
	}

	def newSgxCommunicationInterface(): SgxCommunicator = {
		if (SgxSettings.IS_ENCLAVE) new SocketCommunicator(new Socket(SgxSettings.HOST_IP, SgxSettings.HOST_PORT))
		else new SocketCommunicator(new Socket(SgxSettings.ENCLAVE_IP, SgxSettings.ENCLAVE_PORT))
	}

	def acceptCommunicator(): SgxCommunicator = {
		new SocketCommunicator(server.accept())
	}
}