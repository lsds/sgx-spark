package org.apache.spark.sgx.iterator.socket

import java.net.Socket

import org.apache.spark.sgx.SgxCommunicationInterface
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.iterator.SgxIteratorProviderIdentifier
import org.apache.spark.sgx.sockets.Retry
import org.apache.spark.sgx.sockets.SocketHelper

class SgxSocketIteratorProviderIdentifier(val host: String, val port: Int) extends SgxIteratorProviderIdentifier {
	def connect(): SgxCommunicationInterface = new SocketHelper(Retry(SgxSettings.RETRIES)(new Socket(host, port)))

	override def toString() = getClass.getSimpleName + "(host=" + host + ", port=" + port + ")"
}
