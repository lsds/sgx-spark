package org.apache.spark.sgx

import gnu.trove.map.hash.TLongObjectHashMap

import org.apache.spark.internal.Logging

class IdentifierManager[T]() extends Logging {
	private val identifiers = new TLongObjectHashMap[T]()

	def put[U <: T](id: Long, obj: U): U = this.synchronized {
		identifiers.put(id, obj)
		obj
	}

	def get(id: Long): T = this.synchronized {
	  identifiers.get(id)
	}

	def remove[X](id: Long): X = this.synchronized {
		identifiers.remove(id).asInstanceOf[X]
	}
}