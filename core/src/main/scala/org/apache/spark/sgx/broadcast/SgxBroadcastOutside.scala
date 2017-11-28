package org.apache.spark.sgx.broadcast

import scala.collection.mutable.HashMap

import org.apache.spark.internal.Logging

object SgxBroadcastOutside extends Logging {
  	private val broadcasts = new HashMap[Long,Any]()

  	def put(id: Long, bc: Any): Unit = synchronized {
		broadcasts.put(id, bc)
  		logDebug("put("+id+","+bc+"), length=" + broadcasts.size + " " + broadcasts)
	}

	def get(id: Long): Any = synchronized {
		val x= broadcasts.get(id)
		logDebug("get("+id+") = " + x + " " + x.orNull + " " + x.getOrElse(null) + " " + broadcasts.size + " " + broadcasts)
		x
	}

	def remove(id: Long): Unit = synchronized {
		broadcasts.remove(id)
	}

	override def toString(): String = this.getClass.getSimpleName
}