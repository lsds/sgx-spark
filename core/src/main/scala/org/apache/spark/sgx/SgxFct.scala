/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sgx

import java.util.Comparator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.apache.hadoop.mapred.RecordReader

import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sgx.iterator.{SgxFakeIterator, SgxWritablePartitionedIteratorProvider, SgxWritablePartitionedIteratorProviderIdentifier}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.collection.{PartitionedAppendOnlyMap, SizeTrackingAppendOnlyMap, SizeTrackingAppendOnlyMapIdentifier}

object RecordReaderMaps extends IdentifierManager[RecordReader[_,_]]() {}

object SgxFct extends Logging {
		
  def externalAppendOnlyMapIterator[K,V](id: SizeTrackingAppendOnlyMapIdentifier) = new ExternalAppendOnlyMapIterator[K,V](id).send
  
	def externalSorterInsertAllCreateKey[K](partitioner: Partitioner, pair: Product2[K,Any]) = new ExternalSorterInsertAllCreateKey[K](partitioner, pair).send()

	def partitionedAppendOnlyMapCreate[K,V]() = new PartitionedAppendOnlyMapCreate().send()

	def partitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
			id: SizeTrackingAppendOnlyMapIdentifier,
			keyComparator: Option[Comparator[K]],
			bufOffset: Long,
			bufCapacity: Int) =
		new PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](id, keyComparator, bufOffset, bufCapacity).send()

	def sizeTrackingAppendOnlyMapCreate[K,V]() = new SizeTrackingAppendOnlyMapCreate().send()

	def fct0[Z](fct: () => Z) = new SgxFct0[Z](fct).send().decrypt[Z]

	def fct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) = new SgxFct2[A, B, Z](fct, a, b).send()
}

private case class ExternalSorterInsertAllCreateKey[K](
	partitioner: Partitioner,
	pair: Product2[K,Any]) extends SgxMessage[(Int,K)] {

	def execute() = ThreadUtils.awaitResult( Future {
		(partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1), pair._1)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
}

private case class PartitionedAppendOnlyMapCreate[K,V]() extends SgxMessage[SizeTrackingAppendOnlyMapIdentifier] {

	def execute() = ThreadUtils.awaitResult( Future {
		new PartitionedAppendOnlyMap().id
	}, Duration.Inf)
}

private case class PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
	id: SizeTrackingAppendOnlyMapIdentifier,
	keyComparator: Option[Comparator[K]],
	bufOffset: Long,
	bufCapacity: Int) extends SgxMessage[SgxWritablePartitionedIteratorProviderIdentifier[K,V]] {

	def execute() = ThreadUtils.awaitResult( Future {
		id.getMap.asInstanceOf[PartitionedAppendOnlyMap[K,V]].destructiveSortedWritablePartitionedIterator(keyComparator, bufOffset, bufCapacity).asInstanceOf[SgxWritablePartitionedIteratorProvider[K,V]].getIdentifier
	}, Duration.Inf)
}

private case class SizeTrackingAppendOnlyMapCreate[K,V]() extends SgxMessage[SizeTrackingAppendOnlyMapIdentifier] {

	def execute() = ThreadUtils.awaitResult( Future {
		new SizeTrackingAppendOnlyMap().id
	}, Duration.Inf)
}

private case class ExternalAppendOnlyMapIterator[K,V](
	id: SizeTrackingAppendOnlyMapIdentifier) extends SgxMessage[Iterator[(K,V)]] {
  	
	def execute() = ThreadUtils.awaitResult( Future {
		SgxFakeIterator[(K,V)](id.getMap.iterator)
	}, Duration.Inf)
}

private case class SgxFct0[Z](fct: () => Z) extends SgxMessage[Encrypted] {
	def execute() = ThreadUtils.awaitResult(Future {
		Encrypt(fct())
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "))"
}

private case class SgxFct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) extends SgxMessage[Z] {
	def execute() = ThreadUtils.awaitResult(Future { fct(a, b) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

