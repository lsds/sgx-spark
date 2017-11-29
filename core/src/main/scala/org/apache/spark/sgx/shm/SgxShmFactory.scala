package org.apache.spark.sgx.shm

import org.apache.spark.sgx.Completor
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxFactory
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.broadcast.shm.SgxShmBroadcastProvider
import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.shm.SgxShmIteratorProvider

object SgxShmFactory extends SgxFactory {
	val mgr = if (SgxSettings.IS_ENCLAVE) ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_ENC_TO_OUT, SgxSettings.SHMEM_OUT_TO_ENC);
			else ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_FILE, SgxSettings.SHMEM_SIZE)
	Completor.submit(mgr);

	private var startedBroadcastProvider = false

	def newSgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean): SgxIteratorProvider[T] = {
		val iter = new SgxShmIteratorProvider[T](delegate, doEncrypt)
		Completor.submit(iter)
		iter
	}

	def runSgxBroadcastProvider(): Unit = {
		synchronized {
			if (!startedBroadcastProvider) {
				Completor.submit(new SgxShmBroadcastProvider())
				startedBroadcastProvider = true
			}
		}
	}

	def newSgxCommunicationInterface(): SgxCommunicator = {
		ShmCommunicationManager.get().newShmCommunicator()
	}

	def acceptCommunicator(): SgxCommunicator = {
		ShmCommunicationManager.get().accept()
	}
}