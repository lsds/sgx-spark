package org.apache.spark.sgx

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.broadcast.SgxBroadcastProviderIdentifier

class SgxMainRunner(com: SgxCommunicator) extends SgxCallable[Unit] with Logging {
	def call(): Unit = {
		while(isRunning) {
			val recv = com.recvOne()
			logDebug("Received: " + recv)

			val result = recv match {
				case x: SgxMessage[_] => x.execute()

				case x: SgxBroadcastProviderIdentifier =>
					x.connect()
					true
			}
			
			logDebug("Result: " + result + " (" + result.getClass().getSimpleName + ")")
			if (result != null) com.sendOne(result)
		}

		com.close()
	}

	override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}