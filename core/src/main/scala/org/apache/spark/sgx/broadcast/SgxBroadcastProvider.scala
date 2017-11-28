package org.apache.spark.sgx.broadcast

import java.util.concurrent.Callable

import scala.collection.mutable.HashMap

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.Encrypt
import org.apache.spark.sgx.SgxCommunicator

abstract class SgxBroadcastProvider(broadcasts: HashMap[Long, Any]) extends Callable[Unit] with Logging {
	protected def do_accept: SgxCommunicator

	def call(): Unit = {
		logDebug(this + " now waiting for connections")
		val com = do_accept
		logDebug(this + " got connection: " + com)

		var running = true
		while (running) {
			com.recvOne() match {
				case req: MsgBroadcastGetValue =>
					logDebug("Request for: " + req.id)
					val q = broadcasts.get(req.id)
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
