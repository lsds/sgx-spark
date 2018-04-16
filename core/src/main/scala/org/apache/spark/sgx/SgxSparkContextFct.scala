package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListenerInterface

object SgxSparkContextFct {

  private var _defaultParallelism = -1
  
	def addSparkListener(listener: SparkListenerInterface) = new SgxSparkContextAddSparkListener(listener).send()

	def broadcast[T: ClassTag](value: T) = new SgxSparkContextBroadcast(value).send()

	def conf() = new SgxSparkContextConf().send()

	def create(conf: SparkConf) = new SgxTaskSparkContextCreate(conf).send()

	def defaultParallelism() = {
    if (_defaultParallelism == -1) _defaultParallelism = new SgxSparkContextDefaultParallelism().send()
    _defaultParallelism
  }

  def parallelize[T: ClassTag](seq: Seq[T]) = new SgxSparkContextParallelize(seq).send()

	def stop() = new SgxSparkContextStop().send()

	def textFile(path: String) = new SgxSparkContextTextFile(path).send()
}

private case class SgxSparkContextAddSparkListener(listener: SparkListenerInterface) extends SgxMessage[Unit] {
	def execute() = Await.result( Future { SgxMain.sparkContext.addSparkListener(listener) }, Duration.Inf)
}

private case class SgxSparkContextBroadcast[T: ClassTag](value: T) extends SgxMessage[Broadcast[T]] {
	def execute() = Await.result( Future { SgxMain.sparkContext.broadcast(value) }, Duration.Inf)
}

private case class SgxSparkContextConf() extends SgxMessage[SparkConf] {
	def execute() = Await.result( Future { SgxMain.sparkContext.conf }, Duration.Inf)
}

private case class SgxTaskSparkContextCreate(conf: SparkConf) extends SgxMessage[Unit] {
	def execute() = Await.result( Future { SgxMain.sparkContext = new SparkContext(conf); Unit }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(conf=" + conf + ")"
}

private case class SgxSparkContextDefaultParallelism() extends SgxMessage[Int] {
	def execute() = Await.result( Future { SgxMain.sparkContext.defaultParallelism }, Duration.Inf)
}

private case class SgxSparkContextParallelize[T: ClassTag](seq: Seq[T]) extends SgxMessage[RDD[T]] {
	def execute() = Await.result( Future {
		val rdd = SgxMain.sparkContext.parallelize(seq)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)
}

private case class SgxSparkContextStop() extends SgxMessage[Unit] {
	def execute() = Await.result( Future { SgxMain.sparkContext.stop() }, Duration.Inf)
}

private case class SgxSparkContextTextFile(path: String) extends SgxMessage[RDD[String]] {
	def execute() = Await.result( Future {
		val rdd = SgxMain.sparkContext.textFile(path)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(path=" + path + ")"
}
