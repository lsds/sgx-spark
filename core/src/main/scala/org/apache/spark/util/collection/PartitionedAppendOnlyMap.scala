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

package org.apache.spark.util.collection

import java.util.Comparator

import org.apache.spark.util.collection.WritablePartitionedPairCollection._

import org.apache.spark.internal.Logging
import org.apache.spark.sgx.SgxFct
import org.apache.spark.sgx.SgxSettings

import org.apache.spark.sgx.IdentifierManager
import org.apache.spark.sgx.shm.MappedDataBufferManager

/**
 * Implementation of WritablePartitionedPairCollection that wraps a map in which the keys are tuples
 * of (partition ID, K)
 */
private[spark] class PartitionedAppendOnlyMap[K, V]
  extends SizeTrackingAppendOnlyMap[(Int, K), V] with WritablePartitionedPairCollection[K, V] with Logging {

  override def sgxinit() = {
	  if (SgxSettings.SGX_ENABLED && !SgxSettings.IS_ENCLAVE)
	 	  SgxFct.partitionedAppendOnlyMapCreate()
	  else if (SgxSettings.SGX_ENABLED && SgxSettings.IS_ENCLAVE) {
  	 	val i = scala.util.Random.nextLong
  	 	SizeTrackingAppendOnlyMaps.put(i, this.asInstanceOf[SizeTrackingAppendOnlyMap[Any,Any]])
  	 	new SizeTrackingAppendOnlyMapIdentifier(i)
	  }
	  else new SizeTrackingAppendOnlyMapIdentifier(0)
  }

  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    destructiveSortedIterator(comparator)
  }

  def insert(partition: Int, key: K, value: V): Unit = {
    update((partition, key), value)
  }

  override def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]], bufOffset: Long = -1, bufCapacity: Int = -1)
    : WritablePartitionedIterator = {
    if (SgxSettings.SGX_ENABLED) {
      if (!SgxSettings.IS_ENCLAVE) {
        // We are outside. This happens first.
        // On this side, we consume the values that are produced by the enclave.
        // Call into enclave to prepare the enclave-internal iterator over the data.
        // Before that, we create the shared memory to be used.
        logDebug("xxx destructiveSortedWritablePartitionedIterator outside")
        if (bufOffset != -1 || bufCapacity != -1) throw new IllegalStateException("Something went wrong")
        val buffer = MappedDataBufferManager.get.malloc(33554432)
        SgxFct.partitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](id, keyComparator, buffer.offset(), buffer.capacity()).getIterator()
      }
      else {
        // We are inside the enclave.
        // On this side, we produce/provide the values that are to be consumed outside of the enclave
        logDebug("xxx destructiveSortedWritablePartitionedIterator enclave")
        if (bufOffset == -1 || bufCapacity == -1) throw new IllegalStateException("Something went wrong")
        super.destructiveSortedWritablePartitionedIterator(keyComparator, bufOffset, bufCapacity)
      }
    } else
    super.destructiveSortedWritablePartitionedIterator(keyComparator)
  }
}
