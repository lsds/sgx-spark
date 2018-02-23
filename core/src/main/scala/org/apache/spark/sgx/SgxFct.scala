package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import java.util.Comparator

import org.apache.spark.Partitioner
import org.apache.spark.util.collection.PartitionedAppendOnlyMap
import org.apache.spark.util.collection.PartitionedAppendOnlyMapIdentifier
import org.apache.spark.util.collection.WritablePartitionedIterator

import org.apache.spark.sgx.iterator.SgxWritablePartitionedFakeIterator

object SgxFct {
	def externalSorterInsertAllCreateKey[K](partitioner: Partitioner, pair: Product2[K,Any]) =
		new ExternalSorterInsertAllCreateKey[K](partitioner, pair).send()

	def partitionedAppendOnlyMapCreate[K,V]() =
		new PartitionedAppendOnlyMapCreate().send()

	def partitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
			id: PartitionedAppendOnlyMapIdentifier,
			keyComparator: Option[Comparator[K]]) =
		new PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](id, keyComparator).send()

	def writablePartitionedIteratorHasNext(it: SgxWritablePartitionedFakeIterator) =
		new WritablePartitionedIteratorHasNext(it).send()

	def writablePartitionedIteratorWriteNext(it: SgxWritablePartitionedFakeIterator) =
		new WritablePartitionedIteratorWriteNext(it).send()

	def writablePartitionedIteratorNextPartition(it: SgxWritablePartitionedFakeIterator) =
		new WritablePartitionedIteratorNextPartition(it).send()

	def fct0[Z](fct: () => Z) = new SgxFct0[Z](fct).send()

	def fct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) = new SgxFct2[A, B, Z](fct, a, b).send()
}

private case class ExternalSorterInsertAllCreateKey[K](
	partitioner: Partitioner,
	pair: Product2[K,Any]) extends SgxMessage[(Int,K)] {

	def execute() = Await.result( Future {
		(partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1), pair._1)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
}

private case class PartitionedAppendOnlyMapCreate[K,V]() extends SgxMessage[PartitionedAppendOnlyMapIdentifier] {

	def execute() = Await.result( Future {
		new PartitionedAppendOnlyMap().id
	}, Duration.Inf)
}

private case class PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
	id: PartitionedAppendOnlyMapIdentifier,
	keyComparator: Option[Comparator[K]]) extends SgxMessage[WritablePartitionedIterator] {

	def execute() = SgxWritablePartitionedFakeIterator(
		Await.result( Future {
			id.getMap.destructiveSortedWritablePartitionedIterator(keyComparator)
		}, Duration.Inf)
	)
}

private case class WritablePartitionedIteratorHasNext(
	it: SgxWritablePartitionedFakeIterator) extends SgxMessage[Boolean] {

	def execute() = Await.result( Future {
		it.getIterator.hasNext()
	}, Duration.Inf)
}

private case class WritablePartitionedIteratorNextPartition(
	it: SgxWritablePartitionedFakeIterator) extends SgxMessage[Int] {

	def execute() = Await.result( Future {
		it.getIterator.nextPartition()
	}, Duration.Inf)
}

private case class WritablePartitionedIteratorWriteNext(
	it: SgxWritablePartitionedFakeIterator) extends SgxMessage[Unit] {

	def execute() = Await.result( Future {
		it.getIterator.writeNext(null)
	}, Duration.Inf)
}

private case class SgxFct0[Z](fct: () => Z) extends SgxMessage[Z] {
	def execute() = Await.result(Future { fct() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "))"
}

private case class SgxFct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) extends SgxMessage[Z] {
	def execute() = Await.result(Future { fct(a, b) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

