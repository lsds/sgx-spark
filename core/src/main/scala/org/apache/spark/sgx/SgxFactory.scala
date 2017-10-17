package org.apache.spark.sgx

import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.socket.SgxSocketIteratorProvider

object SgxFactory {
	def newSgxIteratorProvider[T](delegate: Iterator[T], inEnclave: Boolean): SgxIteratorProvider[T] = {
		return new SgxSocketIteratorProvider[T](delegate, inEnclave);
	}
}