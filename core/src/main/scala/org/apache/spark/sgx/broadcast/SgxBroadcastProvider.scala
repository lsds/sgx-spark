package org.apache.spark.sgx.broadcast

import java.util.concurrent.Callable

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.Encrypt
import org.apache.spark.sgx.SgxCommunicator

abstract class SgxBroadcastProvider() extends Callable[Unit] with Logging {
	val com: SgxCommunicator

	def call(): Unit = {
		logDebug(this + " now running with " + com)

		var running = true
		while (running) {
			com.recvOne() match {
				case req: MsgBroadcastGetValue =>
					logDebug("Request for: " + req.id)
					val q = SgxBroadcastOutside.get(req.id)
					logDebug("Sending: " + q)
					com.sendOne(q)

				case MsgBroadcastReqClose =>
					stop(com)
					running = false

				case x: Any => logDebug(this + ": Unknown input message provided.")
			}
		}
	}

	def stop(com: SgxCommunicator) = {
		logDebug(this + ": Stopping ")
		com.close()
	}

	override def toString() = getClass.getSimpleName + "()"
}
