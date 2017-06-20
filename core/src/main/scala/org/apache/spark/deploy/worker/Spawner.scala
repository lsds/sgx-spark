package org.apache.spark.deploy.worker

import scala.collection.JavaConversions._

object Spawner {
	def apply(classname: String): Unit = {
		val sep = System.getProperty("file.separator")
		val classpath = System.getProperty("java.class.path")
		val java = System.getProperty("java.home") + sep + "bin" + sep + "java"

		val processBuilder = new ProcessBuilder(java, "-cp", classpath, classname)
		val process = processBuilder.start()
	}
}
