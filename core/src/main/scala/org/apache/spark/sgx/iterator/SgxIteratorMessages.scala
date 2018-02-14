package org.apache.spark.sgx.iterator

import org.apache.spark.sgx.SgxMessage

case class MsgIteratorReqNextN(num: Int) extends Serializable {}
object MsgIteratorReqClose extends Serializable {}

case class MsgAccessFakeIterator[T](fakeIt: SgxFakeIterator[T]) extends SgxMessage[SgxIteratorProviderIdentifier[Any]] {
	override def execute() = fakeIt.provide()
}
