package org.apache.spark.sgx

import java.util.concurrent.{Executors, CompletionService, Callable, ExecutorCompletionService}

import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.sgx.iterator.SgxFakeIterator

object Completor extends ExecutorCompletionService[Unit](Executors.newFixedThreadPool(32)) {}

class Waiter() extends Callable[Unit] {
       def call(): Unit = while (true) Completor.take
}

object SgxMain {

	def main(args: Array[String]): Unit = {
		val fakeIterators = new IdentifierManager[Iterator[Any],SgxFakeIterator[Any]](SgxFakeIterator(_))
		Completor.submit(new Waiter())

		if (SgxSettings.SGX_USE_SHMEM) {
			while (true) Completor.submit(new SgxMainRunner(SgxFactory.acceptCommunicator(), fakeIterators))
		}
		else {
			try {
				while (true) Completor.submit(new SgxMainRunner(SgxFactory.acceptCommunicator(), fakeIterators))
			}
			finally {
			}
		}
	}
}

