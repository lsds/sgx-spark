package org.apache.spark.deploy.worker

object Spawner {
	// TODO: Obtain these values dynamically
	val classpath = "core/target/scala-2.11/classes"
	val scala = "/usr/bin/scala"
	
	def apply(classname: String): Unit = {		
		logInfo("Spawning: " + processBuilder.command().toString())
		val processBuilder = new ProcessBuilder(scala, "-cp", classpath, classname)
		val process = processBuilder.start()		
	}
}
