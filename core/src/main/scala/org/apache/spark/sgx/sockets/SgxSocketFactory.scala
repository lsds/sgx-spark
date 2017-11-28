package org.apache.spark.sgx.sockets

import java.net.ServerSocket
import java.net.Socket

import org.apache.spark.sgx.Completor
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxFactory
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.socket.SgxSocketIteratorProvider

object SgxSocketFactory extends SgxFactory {

	val server = new ServerSocket(SgxSettings.ENCLAVE_PORT)

	def newSgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean): SgxIteratorProvider[T] = {
		val iter = new SgxSocketIteratorProvider[T](delegate, inEnclave)
		Completor.submit(iter)
		iter
	}

	def runSgxBroadcastProvider(): Unit = {
		throw new UnsupportedOperationException("SgxBroadcastProvider not implemented for socket communucation")
	}

	def newSgxCommunicationInterface(): SgxCommunicator = {
		if (SgxSettings.IS_ENCLAVE) new SocketCommunicator(new Socket(SgxSettings.HOST_IP, SgxSettings.HOST_PORT))
		else new SocketCommunicator(new Socket(SgxSettings.ENCLAVE_IP, SgxSettings.ENCLAVE_PORT))
	}

	def acceptCommunicator(): SgxCommunicator = {
		new SocketCommunicator(server.accept())
	}
}