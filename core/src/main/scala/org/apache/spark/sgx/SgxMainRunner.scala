package org.apache.spark.sgx

import java.util.concurrent.Callable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.broadcast.SgxBroadcastEnclave
import org.apache.spark.sgx.broadcast.SgxBroadcastProviderIdentifier
import org.apache.spark.sgx.iterator.MsgAccessFakeIterator

class SgxMainRunner(com: SgxCommunicator) extends Callable[Unit] with Logging {
	def call(): Unit = {
		while(true) {
			val recv = com.recvOne()
			logDebug("Received: " + recv)

			val result = recv match {
				case x: SgxExecuteInside[_] => x.apply()

				case x: SgxBroadcastProviderIdentifier =>
					logDebug("Accessing broadcast provider " + x)
					x.connect()
					true

				case x: MsgAccessFakeIterator =>
					logDebug("Accessing Fake iterator " + x.fakeId)
					SgxFactory.newSgxIteratorProvider[Any](SgxMain.fakeIterators.remove(x.fakeId), true).identifier
			}
			logDebug("Result: " + result + " (" + result.getClass().getSimpleName + ")")

			if (result != null) com.sendOne(result)
		}

		com.close()
	}

	override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}