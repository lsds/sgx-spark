package org.apache.spark.sgx

import java.util.concurrent.LinkedBlockingDeque

import org.apache.spark.internal.Logging

object ClientHandle extends Logging {
	private val handles = new LinkedBlockingDeque[SgxCommunicator]()

	0 to SgxSettings.CONNECTIONS foreach { _ => handles.add(SgxFactory.newSgxCommunicationInterface()) }

	def sendRecv[O](in: Any) = {
		val h = handles.take
		logDebug("ClientHandle.sendRecv in: " + in)
		val ret = h.sendRecv[O](in)
		logDebug("ClientHandle.sendRecv ret: " + ret)
		handles.add(h)
		ret
	}
}
