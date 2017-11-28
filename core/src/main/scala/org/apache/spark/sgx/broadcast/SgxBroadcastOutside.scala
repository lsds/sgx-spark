package org.apache.spark.sgx.broadcast

import org.apache.spark.internal.Logging

import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxFactory

import scala.collection.mutable.HashMap

object SgxBroadcastOutside {
	private var initialized = false
  	private val broadcasts = new HashMap[Long,Any]()

  	def put(id: Long, bc: Any): Unit = synchronized {
		if (!initialized) {
			SgxFactory.get.runSgxBroadcastProvider(broadcasts)
			initialized = true
		}
		broadcasts.put(id, bc)
	}

	def remove(id: Long): Unit = synchronized {
		broadcasts.remove(id)
	}

	override def toString(): String = this.getClass.getSimpleName
}