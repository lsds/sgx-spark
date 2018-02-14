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

object SgxSparkContextFct {

	def broadcast[T: ClassTag](value: T) = new SgxSparkContextBroadcast(value).send()

	def conf() = new SgxSparkContextConf().send()

	def create(conf: SparkConf) = new SgxTaskSparkContextCreate(conf).send()

	def defaultParallelism() = new SgxSparkContextDefaultParallelism().send()

	def newRddId() = new SgxSparkContextNewRddId().send()

//	def runJob[T, U: ClassTag](
//		rddId: Int,
//    	func: (TaskContext, Iterator[T]) => U,
//    	partitions: Seq[Int],
//    	resultHandler: (Int, U) => Unit) = new SgxTaskSparkContextRunJob(rddId, func, partitions, resultHandler).send()

	def stop() = new SgxSparkContextStop().send()

	def textFile(path: String) = new SgxSparkContextTextFile(path).send()
}

private case class SgxSparkContextBroadcast[T: ClassTag](value: T) extends SgxMessage[Broadcast[T]] {
	def execute() = Await.result( Future { SgxMain.sparkContext.broadcast(value) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxSparkContextConf() extends SgxMessage[SparkConf] {
	def execute() = Await.result( Future { SgxMain.sparkContext.conf }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxTaskSparkContextCreate(conf: SparkConf) extends SgxMessage[Unit] {
	def execute() = Await.result( Future { SgxMain.sparkContext = new SparkContext(conf); Unit }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(conf=" + conf + ")"
}

private case class SgxSparkContextDefaultParallelism() extends SgxMessage[Int] {
	def execute() = Await.result( Future { SgxMain.sparkContext.defaultParallelism }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxSparkContextNewRddId() extends SgxMessage[Int] {
	def execute() = Await.result( Future { SgxMain.sparkContext.newRddId() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

//private case class SgxTaskSparkContextRunJob[T, U: ClassTag](
//	rddId: Int,
//    func: (TaskContext, Iterator[T]) => U,
//    partitions: Seq[Int],
//    resultHandler: (Int, U) => Unit) extends SgxMessage[Unit] {
//
//	def execute() = {
//		SgxMain.sparkContext.runJob(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]], func, partitions, resultHandler(i,u))
//	}
//
//	override def toString = this.getClass.getSimpleName + "()"
//}

private case class SgxSparkContextStop() extends SgxMessage[Unit] {
	def execute() = Await.result( Future { SgxMain.sparkContext.stop() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxSparkContextTextFile(path: String) extends SgxMessage[RDD[String]] {
	def execute() = Await.result( Future {
		val rdd = SgxMain.sparkContext.textFile(path)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(path=" + path + ")"
}