package org.apache.spark.sgx

import java.util.concurrent.LinkedBlockingDeque

object ClientHandle {
	private val handles = new LinkedBlockingDeque[SgxCommunicator]()

	0 to SgxSettings.CONNECTIONS foreach { _ => handles.add(SgxFactory.newSgxCommunicationInterface()) }

	def sendRecv[O](in: Any) = {
		val h = handles.synchronized {
			handles.take
		}
		val ret = h.sendRecv[O](in)
		handles.synchronized {
			handles.add(h)
		}
		ret
	}
}