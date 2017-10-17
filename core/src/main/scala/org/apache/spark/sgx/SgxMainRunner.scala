package org.apache.spark.sgx

import java.util.concurrent.Callable

import org.apache.spark.sgx.iterator.MsgAccessFakeIterator
import org.apache.spark.sgx.iterator.SgxFakeIterator

class SgxMainRunner(com: SgxCommunicationInterface, fakeIterators: IdentifierManager[Iterator[Any],SgxFakeIterator[Any]]) extends Callable[Unit] {
	def call(): Unit = {
		while(true) {
			com.sendOne(com.recvOne() match {
				case x: SgxFct2[_,_,_] => x.apply()
				case x: SgxFirstTask[_,_] => fakeIterators.create(x.apply())
				case x: SgxOtherTask[_,_] => fakeIterators.create(x.apply(fakeIterators.remove(x.it.id)))

				case x: MsgAccessFakeIterator =>
					val iter = SgxFactory.newSgxIteratorProvider[Any](fakeIterators.get(x.fakeId), true)
					new Thread(iter).start
					iter.identifier
			})
		}

		com.close()
	}

	override def toString() = getClass.getSimpleName + "(com=" + com + ")"
}