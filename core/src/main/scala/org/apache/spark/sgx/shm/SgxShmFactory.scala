package org.apache.spark.sgx.shm

import org.apache.spark.sgx.Completor
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxFactory
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.shm.SgxShmIteratorProvider

object SgxShmFactory extends SgxFactory {
	val mgr = ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_ENC_TO_OUT, SgxSettings.SHMEM_OUT_TO_ENC);
	Completor.submit(mgr);

	def newSgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean): SgxIteratorProvider[T] = {
		new SgxShmIteratorProvider[T](delegate, inEnclave)
	}

	def newSgxCommunicationInterface(): SgxCommunicator = {
		ShmCommunicationManager.get().newShmCommunicator()
	}

	def acceptCommunicator(): SgxCommunicator = {
		ShmCommunicationManager.get().accept()
	}
}