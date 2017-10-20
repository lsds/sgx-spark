package org.apache.spark.sgx

import org.apache.spark.sgx.iterator.SgxIteratorProvider

trait SgxFactory {
	def newSgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean): SgxIteratorProvider[T]

	def newSgxCommunicationInterface(): SgxCommunicator

	def acceptCommunicator(): SgxCommunicator
}

object SgxFactory {
	private val factory = if (SgxSettings.SGX_USE_SHMEM) SgxShmFactory else SgxSocketFactory

	def get() = factory
}