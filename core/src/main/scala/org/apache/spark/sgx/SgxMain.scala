package org.apache.spark.sgx

import java.util.concurrent.{Executors, Callable, ExecutorCompletionService}

import org.apache.spark.sgx.iterator.SgxFakeIterator

object Completor extends ExecutorCompletionService[Unit](Executors.newCachedThreadPool()) {}

class Waiter() extends Callable[Unit] {
       def call(): Unit = while (true) Completor.take
}

object SgxMain {

	def main(args: Array[String]): Unit = {
		val fakeIterators = new IdentifierManager[Iterator[Any],SgxFakeIterator[Any]](SgxFakeIterator(_))
		Completor.submit(new Waiter())

		while (true) Completor.submit(new SgxMainRunner(SgxFactory.get.acceptCommunicator(), fakeIterators))
	}
}

