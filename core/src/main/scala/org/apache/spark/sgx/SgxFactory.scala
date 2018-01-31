package org.apache.spark.sgx

import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.iterator.SgxIteratorProvider

import org.apache.spark.sgx.shm.SgxShmFactory

trait SgxFactory {
	def newSgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean): SgxIteratorProvider[T]

	def runSgxBroadcastProvider(): Unit

	def newSgxCommunicationInterface(): SgxCommunicator

	def acceptCommunicator(): SgxCommunicator
}

object SgxFactory {
	private val factory = SgxShmFactory
	def get() = factory
}