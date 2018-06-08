package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import java.util.Comparator

import org.apache.spark.Partitioner
import org.apache.spark.util.collection.PartitionedAppendOnlyMap
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMapIdentifier
import org.apache.spark.util.collection.WritablePartitionedIterator

import org.apache.spark.storage.DiskBlockObjectWriter

import org.apache.spark.sgx.iterator.SgxIteratorIdentifier
import org.apache.spark.sgx.iterator.SgxFakeIterator
import org.apache.spark.sgx.iterator.SgxIterator

import org.apache.hadoop.mapred.RecordReader

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.SgxWritablePartitionedIteratorProvider
import org.apache.spark.sgx.iterator.SgxWritablePartitionedIteratorProviderIdentifier

object RecordReaderMaps extends IdentifierManager[RecordReader[_,_]]() {}

object SgxFct extends Logging {
		
  def externalAppendOnlyMapIterator[K,V](id: SizeTrackingAppendOnlyMapIdentifier) =
    new ExternalAppendOnlyMapIterator[K,V](id).send
  
	def externalSorterInsertAllCreateKey[K](partitioner: Partitioner, pair: Product2[K,Any]) =
		new ExternalSorterInsertAllCreateKey[K](partitioner, pair).send()

  def getPartitionFirstOfPair(partitioner: Partitioner, enc: Encrypted) =
    new GetPartitionFirstOfPair(partitioner, enc).send()

	def partitionedAppendOnlyMapCreate[K,V]() =
		new PartitionedAppendOnlyMapCreate().send()

	def partitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
			id: SizeTrackingAppendOnlyMapIdentifier,
			keyComparator: Option[Comparator[K]],
			bufOffset: Long,
			bufCapacity: Int) =
		new PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](id, keyComparator, bufOffset, bufCapacity).send()

	def sizeTrackingAppendOnlyMapCreate[K,V]() =
		new SizeTrackingAppendOnlyMapCreate().send()

	def fct0[Z](fct: () => Z) = new SgxFct0[Z](fct).send().decrypt[Z]

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

private case class GetPartitionFirstOfPair(partitioner: Partitioner, enc: Encrypted) extends SgxMessage[Int] {

  def execute() = Await.result( Future {
		partitioner.getPartition(enc.decrypt[Product2[Any,Any]]._1)
	}, Duration.Inf)
}

private case class PartitionedAppendOnlyMapCreate[K,V]() extends SgxMessage[SizeTrackingAppendOnlyMapIdentifier] {

	def execute() = Await.result( Future {
		new PartitionedAppendOnlyMap().id
	}, Duration.Inf)
}

private case class PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
	id: SizeTrackingAppendOnlyMapIdentifier,
	keyComparator: Option[Comparator[K]],
  bufOffset: Long,
	bufCapacity: Int) extends SgxMessage[SgxWritablePartitionedIteratorProviderIdentifier[K,V]] {

	def execute() = Await.result( Future {
    id.getMap.asInstanceOf[PartitionedAppendOnlyMap[K,V]].destructiveSortedWritablePartitionedIterator(keyComparator, bufOffset, bufCapacity).asInstanceOf[SgxWritablePartitionedIteratorProvider[K,V]].getIdentifier
	}, Duration.Inf)
}

private case class SizeTrackingAppendOnlyMapCreate[K,V]() extends SgxMessage[SizeTrackingAppendOnlyMapIdentifier] {

	def execute() = Await.result( Future {
		new SizeTrackingAppendOnlyMap().id
	}, Duration.Inf)
}

private case class ExternalAppendOnlyMapIterator[K,V](
    id: SizeTrackingAppendOnlyMapIdentifier) extends SgxMessage[Iterator[(K,V)]] {
  	
  def execute() = Await.result( Future {
    SgxFakeIterator[(K,V)](id.getMap.iterator)
	}, Duration.Inf)
}

private case class SgxFct0[Z](fct: () => Z) extends SgxMessage[Encrypted] {
	def execute() = Await.result(Future { Encrypt(fct()) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "))"
}

private case class SgxFct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) extends SgxMessage[Z] {
	def execute() = Await.result(Future { fct(a, b) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

