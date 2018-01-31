package org.apache.spark.sgx.broadcast

import java.util.concurrent.Callable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging

import org.apache.spark.sgx.ClientHandle
import org.apache.spark.sgx.shm.ShmCommunicationManager

import org.apache.spark.sgx.Encrypt
import org.apache.spark.sgx.SgxCommunicator

class SgxBroadcastProvider() extends Callable[Unit] with Logging {

	private val _com = ShmCommunicationManager.get().newShmCommunicator(false)

	ClientHandle.sendRecv[Boolean](new SgxBroadcastProviderIdentifier(_com.getMyPort))

	val com = _com.connect(_com.recvOne.asInstanceOf[Long])

	def call(): Unit = {
		logDebug(this + " now running with " + com)

		var running = true
		while (running) {
			val r = com.recvOne() match {
				case req: MsgBroadcast[_] =>
					req.apply

				case MsgBroadcastReqClose =>
					stop(com)
					running = false
					null

				case x: Any =>
					logDebug(this + ": Unknown input message provided.")
					null
			}

			if (r != null) com.sendOne(r)
		}
	}

	def stop(com: SgxCommunicator) = {
		logDebug(this + ": Stopping ")
		com.close()
	}

	override def toString() = this.getClass.getSimpleName + "(com=" + com + ")"
}
