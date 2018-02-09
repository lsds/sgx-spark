package org.apache.spark.sgx.iterator

case class MsgIteratorReqNextN(num: Int) extends Serializable {}
object MsgIteratorReqClose extends Serializable {}

case class MsgAccessFakeIterator[T](fakeIt: SgxFakeIterator[T]) extends Serializable {}