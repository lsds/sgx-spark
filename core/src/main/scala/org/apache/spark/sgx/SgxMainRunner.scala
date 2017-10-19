package org.apache.spark.sgx

import java.util.concurrent.Callable

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.MsgAccessFakeIterator
import org.apache.spark.sgx.iterator.SgxFakeIterator

class SgxMainRunner(com: SgxCommunicationInterface, fakeIterators: IdentifierManager[Iterator[Any],SgxFakeIterator[Any]]) extends Callable[Unit] with Logging {
	def call(): Unit = {
		while(true) {
			logDebug(this + " waiting for message")
			val r = com.recvOne()
			logDebug(this + " received: " + r)
			val y = r match {
				case x: SgxFct2[_,_,_] => x.apply()
				case x: SgxFirstTask[_,_] => {
					logDebug(this + " FirstTask start")
					val b = fakeIterators.create(x.apply())
					logDebug(this + " FirstTask end")
					b
				}
				case x: SgxOtherTask[_,_] => {
					logDebug(this + " OtherTask start")
					val b = fakeIterators.create(x.apply(fakeIterators.remove(x.it.id)))
					logDebug(this + " OtherTask end")
					b
				}

				case x: MsgAccessFakeIterator =>
					val iter = SgxFactory.newSgxIteratorProvider[Any](fakeIterators.get(x.fakeId), true)
					new Thread(iter).start
					iter.identifier
			}
			logDebug(this + " answer for "+r+": " + y)
			com.sendOne(y)
			logDebug(this + " answer sent")
		}

		logDebug(this + " closing")
		com.close()
	}

	override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}