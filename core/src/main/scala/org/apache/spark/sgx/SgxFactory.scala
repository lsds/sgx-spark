package org.apache.spark.sgx

import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.iterator.SgxIteratorProv
import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.SgxShmIteratorProvider
import org.apache.spark.sgx.shm.ShmCommunicationManager

import org.apache.spark.util.NextIterator
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.mapred.RecordReader
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.executor.InputMetrics
import org.apache.spark.Partition
import org.apache.spark.util.collection.WritablePartitionedIterator
import org.apache.spark.sgx.iterator.SgxWritablePartitionedIteratorProvider

object SgxFactory {
	val mgr =
	if (SgxSettings.IS_ENCLAVE) {
		if (SgxSettings.DEBUG_IS_ENCLAVE_REAL) ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_ENC_TO_OUT, SgxSettings.SHMEM_OUT_TO_ENC, SgxSettings.SHMEM_COMMON, SgxSettings.SHMEM_SIZE)
		else ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_FILE, SgxSettings.SHMEM_SIZE)
	}
	else ShmCommunicationManager.create[Unit](SgxSettings.SHMEM_FILE, SgxSettings.SHMEM_SIZE)
	Completor.submit(mgr);

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
		ShmCommunicationManager.get().newShmCommunicator()
	}

	def acceptCommunicator(): SgxCommunicator = {
		ShmCommunicationManager.get().accept()
	}
}