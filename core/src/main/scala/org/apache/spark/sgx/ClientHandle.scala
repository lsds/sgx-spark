package org.apache.spark.sgx

import java.util.concurrent.LinkedBlockingDeque

import org.apache.spark.internal.Logging

object ClientHandle extends Logging {
	private val handles = new LinkedBlockingDeque[SgxCommunicator]()


	System.err.println("ClientHandle.Create new handles from 0 to " + SgxSettings.CONNECTIONS)
	//0 to SgxSettings.CONNECTIONS foreach { _ => handles.add(SgxFactory.newSgxCommunicationInterface()) }
	handles.add(SgxFactory.newSgxCommunicationInterface())

	def sendRecv[O](in: Any) = {
		System.err.println("ClientHandle.sendRecv1, in = " + in)
		val h = handles.take
		System.err.println("ClientHandle.sendRecv2, h = " + h)
			val ret = h.sendRecv[O](in)
		System.err.println("ClientHandle.sendRecv3, ret = " + ret)
		handles.add(h)
		ret
	}
}
