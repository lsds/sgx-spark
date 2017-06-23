package org.apache.spark.sgx

import scala.collection.JavaConversions._

object SgxSpawn {
	def apply(): Unit = {
		val sep = System.getProperty("file.separator")
		val java = System.getProperty("java.home") + sep + "bin" + sep + "java"
		val classpath = System.getProperty("java.class.path")
		val classname = SgxMain.getClass().getCanonicalName().dropRight(1)

		val processBuilder = new ProcessBuilder(java, "-cp", classpath, classname)
//		val process = processBuilder.start()
	}
}
