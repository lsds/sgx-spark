package org.apache.spark.sgx

import org.apache.spark.sgx.broadcast.SgxBroadcastProvider
import org.apache.spark.sgx.iterator.SgxIteratorProv
import org.apache.spark.sgx.iterator.SgxIteratorProvider
import org.apache.spark.sgx.iterator.SgxShmIteratorProvider
import org.apache.spark.sgx.shm.ShmCommunicationManager

import org.apache.spark.util.NextIterator
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.mapred.RecordReader


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
	
	def newSgxShmIteratorProvider[K,V](delegate: NextIterator[(K,V)], recordReader: RecordReader[K,V]): SgxIteratorProv[(K,V)] = {
		val prov = new SgxShmIteratorProvider[K,V](delegate, recordReader.getLineReader.getBufferOffset(), recordReader.getLineReader.getBufferSize())		
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