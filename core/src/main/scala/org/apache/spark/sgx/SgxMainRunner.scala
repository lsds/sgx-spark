package org.apache.spark.sgx

import java.util.concurrent.Callable

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.MsgAccessFakeIterator
import org.apache.spark.sgx.iterator.SgxFakeIterator

class SgxMainRunner(com: SgxCommunicator) extends Callable[Unit] with Logging {
	def call(): Unit = {
		while(true) {
			val recv = com.recvOne()
			logDebug("Received: " + recv)
			val result = recv match {
				case x: SgxFct2[_,_,_] => x.apply()
				case x: SgxFirstTask[_,_] => SgxMain.fakeIterators.create(x.apply())
				case x: SgxOtherTask[_,_] => SgxMain.fakeIterators.create(x.apply())
				case x: SgxComputeTaskZippedPartitionsRDD2[_,_,_] => SgxMain.fakeIterators.create(x.apply())
				case x: SgxComputeTaskPartitionwiseSampledRDD[_,_] => SgxMain.fakeIterators.create(x.apply())

				case x: MsgAccessFakeIterator =>
					SgxFactory.get.newSgxIteratorProvider[Any](SgxMain.fakeIterators.get(x.fakeId), true).identifier
			}
			logDebug("Result: " + result + " (" + result.getClass().getSimpleName + ")")
			com.sendOne(result)
		}

		com.close()
	}

	override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}