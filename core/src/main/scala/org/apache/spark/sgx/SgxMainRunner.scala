package org.apache.spark.sgx

import java.util.concurrent.Callable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.broadcast.SgxBroadcastEnclave
import org.apache.spark.sgx.broadcast.SgxBroadcastProviderIdentifier
import org.apache.spark.sgx.iterator.MsgAccessFakeIterator
import org.apache.spark.sgx.iterator.SgxFakeIterator

class SgxMainRunner(com: SgxCommunicator) extends Callable[Unit] with Logging {
	def call(): Unit = {
		while(true) {
			val recv = com.recvOne()
			logDebug("Received: " + recv)

			val result = recv match {
				case x: SgxExecuteInside[_] => x.apply() match {
					case r: SgxFakeIterator[_] => r
					case a: Any => Encrypt(a)
				}

				case x: SgxBroadcastProviderIdentifier =>
					logDebug("Accessing broadcast provider " + x)
					x.connect()
					true

				case x: MsgAccessFakeIterator =>
					logDebug("Accessing Fake iterator " + x.fakeId)
					SgxFactory.get.newSgxIteratorProvider[Any](SgxMain.fakeIterators.get(x.fakeId), true).identifier
			}
			logDebug("Result: " + result + " (" + result.getClass().getSimpleName + ") as result of " + recv)

			if (result != null) {
//				val r = result match {
//					case d: Double => HandleManager.create(scala.util.Random.nextDouble, d)
//					case x: Any => x
//				}
				com.sendOne(result)
			}
		}

		com.close()
	}

	override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}