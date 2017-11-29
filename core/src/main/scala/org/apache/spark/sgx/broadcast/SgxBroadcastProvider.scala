package org.apache.spark.sgx.broadcast

import java.util.concurrent.Callable

import org.apache.spark.broadcast.Broadcast
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
				case req: MsgBroadcastGet[_] =>
					logDebug("Request for: " + req.bc)
					req.bc.value
					logDebug("Sending: " + req.bc.value)
					com.sendOne(req.bc.value)

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
