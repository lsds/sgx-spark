package org.apache.spark.sgx

import gnu.trove.map.hash.TLongObjectHashMap

class IdentifierManager[T,F](c: Long => F) {
	private val identifiers = new TLongObjectHashMap[T]()

	def create(obj: T): F = this.synchronized {
		val uuid = scala.util.Random.nextLong
		identifiers.put(uuid, obj)
		c(uuid)
	}

	def get(id: Long): T = this.synchronized {
		identifiers.get(id)
	}

	def remove(id: Long): T = this.synchronized {
		identifiers.remove(id)
	}
}