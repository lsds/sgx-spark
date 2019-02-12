/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sgx

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListenerInterface
import org.apache.spark.util.ThreadUtils

object SgxSparkContextFct {

	private var _defaultParallelism = -1
	private var _conf: SparkConf = null.asInstanceOf[SparkConf]
  
	def addSparkListener(listener: SparkListenerInterface) = new SgxSparkContextAddSparkListener(listener).send()

	def broadcast[T: ClassTag](value: T) = new SgxSparkContextBroadcast(value).send()

	def conf() = {
		if (_conf == null) _conf = new SgxSparkContextConf().send()
		_conf
	}

	def create(conf: SparkConf) = new SgxTaskSparkContextCreate(conf).send()

	def defaultParallelism() = {
		if (_defaultParallelism == -1) _defaultParallelism = new SgxSparkContextDefaultParallelism().send()
		_defaultParallelism
	}
	
	def hadoopConfigurationSet(name: String, value: String) = new SgxSparkContextHadoopConfigurationSet(name, value).send()

	def parallelize[T: ClassTag](seq: Seq[T]) = new SgxSparkContextParallelize(seq).send()

	def stop() = new SgxSparkContextStop().send()

	def textFile(path: String) = new SgxSparkContextTextFile(path).send()
}

private case class SgxSparkContextAddSparkListener(listener: SparkListenerInterface) extends SgxMessage[Unit] {
	def execute() = ThreadUtils.awaitResult( Future { SgxMain.sparkContext.addSparkListener(listener) }, Duration.Inf)
}

private case class SgxSparkContextBroadcast[T: ClassTag](value: T) extends SgxMessage[Broadcast[T]] {
	def execute() = ThreadUtils.awaitResult( Future { SgxMain.sparkContext.broadcast(value) }, Duration.Inf)
}

private case class SgxSparkContextConf() extends SgxMessage[SparkConf] {
	def execute() = ThreadUtils.awaitResult( Future { SgxMain.sparkContext.conf }, Duration.Inf)
}

private case class SgxTaskSparkContextCreate(conf: SparkConf) extends SgxMessage[Unit] {
	def execute() = ThreadUtils.awaitResult( Future { SgxMain.sparkContext = new SparkContext(conf); Unit }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(conf=" + conf + ")"
}

private case class SgxSparkContextDefaultParallelism() extends SgxMessage[Int] {
	def execute() = ThreadUtils.awaitResult( Future { SgxMain.sparkContext.defaultParallelism }, Duration.Inf)
}

private case class SgxSparkContextHadoopConfigurationSet(name: String, value: String) extends SgxMessage[Unit] {
	def execute() = ThreadUtils.awaitResult( Future { SgxMain.sparkContext.hadoopConfiguration.set(name, value) }, Duration.Inf)
}


private case class SgxSparkContextParallelize[T: ClassTag](seq: Seq[T]) extends SgxMessage[RDD[T]] {
	def execute() = ThreadUtils.awaitResult( Future {
		val rdd = SgxMain.sparkContext.parallelize(seq)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)
}

private case class SgxSparkContextStop() extends SgxMessage[Unit] {
	def execute() = ThreadUtils.awaitResult( Future { SgxMain.sparkContext.stop() }, Duration.Inf)
}

private case class SgxSparkContextTextFile(path: String) extends SgxMessage[RDD[String]] {
	def execute() = ThreadUtils.awaitResult( Future {
		val rdd = SgxMain.sparkContext.textFile(path)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(path=" + path + ")"
}
