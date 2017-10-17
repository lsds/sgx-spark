package org.apache.spark.sgx

import java.net.Socket
import java.util.concurrent.LinkedBlockingDeque

import org.apache.spark.sgx.sockets.SocketHelper

object ClientHandle {
	private val handles = new LinkedBlockingDeque[SgxCommunicationInterface]()

	0 to SgxSettings.CONNECTIONS foreach { _ =>
		if (SgxSettings.SGX_USE_SHMEM) handles.add(ShmCommunicationManager.get().newShmCommunicator())
		else {
			if (SgxSettings.IS_ENCLAVE) handles.add(new SocketHelper(new Socket(SgxSettings.HOST_IP, SgxSettings.HOST_PORT)))
			else handles.add(new SocketHelper(new Socket(SgxSettings.ENCLAVE_IP, SgxSettings.ENCLAVE_PORT)))
		}
	}

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