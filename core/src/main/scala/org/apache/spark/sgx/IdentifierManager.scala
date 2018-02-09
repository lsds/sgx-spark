package org.apache.spark.sgx

import gnu.trove.map.hash.TLongObjectHashMap

import org.apache.spark.internal.Logging

class IdentifierManager[T]() extends Logging {
	private val identifiers = new TLongObjectHashMap[T]()

	def put[U <: T](id: Long, obj: U): U = this.synchronized {
		logDebug("put("+id+","+obj+")")
		identifiers.put(id, obj)
		obj
	}

	def get(id: Long): T = this.synchronized {
		logDebug("get("+id+")")
		val x = identifiers.get(id)
		logDebug("get("+id+") = " + x)
		x
	}

	def remove[X](id: Long): X = this.synchronized {
		logDebug("remove("+id+")")
		identifiers.remove(id).asInstanceOf[X]
	}
}