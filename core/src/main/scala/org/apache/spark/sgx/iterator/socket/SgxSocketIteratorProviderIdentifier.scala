package org.apache.spark.sgx.iterator.socket

import java.net.Socket

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.sockets.Retry
import org.apache.spark.sgx.sockets.SocketCommunicator

class SgxSocketIteratorProviderIdentifier(val host: String, val port: Int) extends SgxIteratorProviderIdentifier {
	def connect(): SgxCommunicator = new SocketCommunicator(Retry(SgxSettings.RETRIES)(new Socket(host, port)))

	override def toString() = getClass.getSimpleName + "(host=" + host + ", port=" + port + ")"
}
