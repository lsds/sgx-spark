package org.apache.spark.sgx.iterator.socket

import java.net.ServerSocket

import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.sockets.Retry
import org.apache.spark.sgx.sockets.SocketCommunicator

class SgxSocketIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean) extends SgxIteratorProvider[T](delegate, inEnclave) {
	val host = if (inEnclave) SgxSettings.ENCLAVE_IP else SgxSettings.HOST_IP
	val port = 40000 + scala.util.Random.nextInt(10000)
	val identifier = new SgxSocketIteratorProviderIdentifier(host, port)

	override def do_accept() = new SocketCommunicator(new ServerSocket(port).accept())

	override def toString() = this.getClass.getSimpleName + "(host=" + host + ", port=" + port + ", identifier=" + identifier + ")"
}