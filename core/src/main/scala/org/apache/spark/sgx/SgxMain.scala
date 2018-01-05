package org.apache.spark.sgx

import java.util.concurrent.{Executors, Callable, ExecutorCompletionService}
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.SgxFakeIterator

import org.apache.spark.SparkContext


object Completor extends ExecutorCompletionService[Unit](Executors.newCachedThreadPool()) {}

class Waiter() extends Callable[Unit] {
       def call(): Unit = while (true) Completor.take
}

object SgxMain extends Callable[Unit] with Logging {
	val fakeIterators = new IdentifierManager[Iterator[Any],SgxFakeIterator[Any]](SgxFakeIterator(_))
	var sparkContext: SparkContext = _

	def main(args: Array[String]): Unit = {
		logDebug("Running SgxMain.main()")
		Completor.submit(new Waiter())
		while (true) Completor.submit(new SgxMainRunner(SgxFactory.get.acceptCommunicator()))
	}

	def call(): Unit = main(null)
}

