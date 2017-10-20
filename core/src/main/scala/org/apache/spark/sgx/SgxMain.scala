package org.apache.spark.sgx

import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.{Executors, CompletionService, Callable, ExecutorCompletionService}

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.sgx.sockets.SocketHelper
import org.apache.spark.sgx.iterator.SgxFakeIterator

object Completor extends ExecutorCompletionService[Unit](Executors.newFixedThreadPool(32)) {}

class Waiter() extends Callable[Unit] {
       def call(): Unit = while (true) Completor.take
}

object SgxMain extends Logging {

	def main(args: Array[String]): Unit = {
		val fakeIterators = new IdentifierManager[Iterator[Any],SgxFakeIterator[Any]](SgxFakeIterator(_))
		Completor.submit(new Waiter())

		if (SgxSettings.SGX_USE_SHMEM) {
			val mgr = ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_ENC_TO_OUT, SgxSettings.SHMEM_OUT_TO_ENC);
			Completor.submit(mgr);

			while (true) Completor.submit(new SgxMainRunner(mgr.accept(), fakeIterators))
		}
		else {
			val server = new ServerSocket(SgxSettings.ENCLAVE_PORT)
			logDebug("Main: Waiting for connections on port " + server.getLocalPort)

			try {
				while (true) Completor.submit(new SgxMainRunner(new SocketHelper(server.accept()), fakeIterators))
			}
			finally {
				server.close()
			}
		}
	}
}

