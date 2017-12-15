//package org.apache.spark.sgx
//
//import scala.collection.mutable.HashMap
//
//import org.apache.spark.internal.Logging
//
//object HandleManager extends Logging {
//	val map = new HashMap[Any,Any]
//
//	def create(a: Any, b: Any) = {
//		logDebug("xxx create("+a+", "+b+")")
//		map.put(a, b)
//		a
//	}
//
//	def get[T](a: Any) = {
//		logDebug("xxx get("+a+")")
//		val r = map.get(a).get.asInstanceOf[T]
//		logDebug("xxx get("+a+") = " + r)
//		r
//	}
//}
//
//object SgxPrimitiveHandles {
//	implicit class HandleDouble(val d: Double) {
//		def value: Double = HandleManager.get[Double](d)
//	}
//}