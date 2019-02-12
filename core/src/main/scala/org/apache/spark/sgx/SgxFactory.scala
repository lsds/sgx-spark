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

import org.apache.hadoop.mapred.RecordReader

import org.apache.spark.Partition
import org.apache.spark.executor.InputMetrics
import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.iterator.{SgxIteratorProv, SgxIteratorProvider, SgxShmIteratorProvider, SgxWritablePartitionedIteratorProvider}
import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.util.NextIterator
import org.apache.spark.util.collection.WritablePartitionedIterator

object SgxFactory {
	val mgr =
	if (SgxSettings.IS_ENCLAVE) {
		if (SgxSettings.DEBUG_IS_ENCLAVE_REAL) {
			ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_ENC_TO_OUT, SgxSettings.SHMEM_OUT_TO_ENC, SgxSettings.SHMEM_COMMON, SgxSettings.SHMEM_SIZE)
		} else {
			ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_FILE, SgxSettings.SHMEM_SIZE)
		}
	}	else {
		ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_FILE, SgxSettings.SHMEM_SIZE)
	}
	Completor.submit(mgr);
	if (!SgxSettings.IS_ENCLAVE) Completor.submit(SgxMain)

	private var startedBroadcastProvider = false

	def newSgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean): SgxIteratorProv[T] = {
		val iter = new SgxIteratorProvider[T](delegate, doEncrypt)
		Completor.submit(iter)
		iter
	}
	
	def newSgxShmIteratorProvider[K,V](delegate: NextIterator[(K,V)], recordReader: RecordReader[K,V], theSplit: Partition, inputMetrics: InputMetrics, splitLength: Long, splitStart: Long, delimiter: Array[Byte]): SgxIteratorProv[(K,V)] = {
		val prov = new SgxShmIteratorProvider[K,V](delegate, recordReader, theSplit, inputMetrics, splitLength, splitStart, delimiter)
		Completor.submit(prov)
		prov
	}
	
	def newSgxWritablePartitionedIteratorProvider[K,V](delegate: Iterator[Product2[Product2[Int,K],V]], offset: Long, size: Int): WritablePartitionedIterator = {
		val prov = new SgxWritablePartitionedIteratorProvider(delegate, offset, size)
		Completor.submit(prov)
		prov
	}

	def runSgxBroadcastProvider(): Unit = {
		synchronized {
			if (!startedBroadcastProvider) {
				Completor.submit(new SgxBroadcastProvider())
				startedBroadcastProvider = true
			}
		}
	}

	def newSgxCommunicationInterface(): SgxCommunicator = {
		ShmCommunicationManager.newShmCommunicator()
	}

	def acceptCommunicator(): SgxCommunicator = {
		ShmCommunicationManager.accept()
	}
}