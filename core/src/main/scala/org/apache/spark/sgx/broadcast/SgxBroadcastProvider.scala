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
			val r = com.recvOne() match {
				case req: MsgBroadcastValue[_] =>
					logDebug("Request for: " + req.bc + ".value")
					req.bc.value

				case req: MsgBroadcastDestroy[_] =>
					logDebug("Request for: " + req.bc + ".destroy")
					req.bc.destroy

				case req: MsgBroadcastUnpersist[_] =>
					logDebug("Request for: " + req.bc + ".unpersist")
					req.bc.unpersist

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

	override def toString() = getClass.getSimpleName + "()"
}
