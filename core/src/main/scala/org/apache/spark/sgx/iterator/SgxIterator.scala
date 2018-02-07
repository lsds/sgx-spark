package org.apache.spark.sgx.iterator

trait SgxIterator[T] {
	def getIdentifier: SgxIteratorIdentifier[T]
}

trait SgxIteratorIdentifier[T] extends Serializable {
	def getIterator: Iterator[T]
}