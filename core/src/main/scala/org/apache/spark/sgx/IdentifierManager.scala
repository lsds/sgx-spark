package org.apache.spark.sgx

import org.apache.spark.rdd.ShuffledRDD

import gnu.trove.map.hash.TLongObjectHashMap

class IdentifierManager[T,F](c: Long => F) {
	private val identifiers = new TLongObjectHashMap[T]()

	def create(obj: T): F = this.synchronized {
		val id = scala.util.Random.nextLong
		identifiers.put(id, obj)
		c(id)
	}

	def get(id: Long): T = this.synchronized {
		identifiers.get(id)
	}

	def remove[X](id: Long): X = this.synchronized {
		identifiers.remove(id).asInstanceOf[X]
	}
}

class ShuffledRDDManager {
	private val identifiers = new TLongObjectHashMap[ShuffledRDD[_,_,_]]()

	def put[K,V,C](id: Long, obj: ShuffledRDD[K,V,C]) = this.synchronized {
		identifiers.put(id, obj)
	}

	def get[K,V,C](id: Long) = this.synchronized {
		identifiers.get(id).asInstanceOf[ShuffledRDD[K,V,C]]
	}

	def remove[K,V,C](id: Long) = this.synchronized {
		identifiers.remove(id).asInstanceOf[ShuffledRDD[K,V,C]]
	}
}
