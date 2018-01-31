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

	def broadcast[T: ClassTag](value: T) = new SgxSparkContextBroadcast(value).executeInsideEnclave()

	def conf() = new SgxSparkContextConf().executeInsideEnclave()

	def create(conf: SparkConf) = new SgxTaskSparkContextCreate(conf).executeInsideEnclave()

	def defaultParallelism() = new SgxSparkContextDefaultParallelism().executeInsideEnclave()

	def newRddId() = new SgxSparkContextNewRddId().executeInsideEnclave()

//	def runJob[T, U: ClassTag](
//		rddId: Int,
//    	func: (TaskContext, Iterator[T]) => U,
//    	partitions: Seq[Int],
//    	resultHandler: (Int, U) => Unit) = new SgxTaskSparkContextRunJob(rddId, func, partitions, resultHandler).executeInsideEnclave()

	def stop() = new SgxSparkContextStop().executeInsideEnclave()

	def textFile(path: String) = new SgxSparkContextTextFile(path).executeInsideEnclave()
}

private case class SgxSparkContextBroadcast[T: ClassTag](value: T) extends SgxExecuteInside[Broadcast[T]] {
	def apply() = Await.result( Future { SgxMain.sparkContext.broadcast(value) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxSparkContextConf() extends SgxExecuteInside[SparkConf] {
	def apply() = Await.result( Future { SgxMain.sparkContext.conf }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxTaskSparkContextCreate(conf: SparkConf) extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.sparkContext = new SparkContext(conf); Unit }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(conf=" + conf + ")"
}

private case class SgxSparkContextDefaultParallelism() extends SgxExecuteInside[Int] {
	def apply() = Await.result( Future { SgxMain.sparkContext.defaultParallelism }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxSparkContextNewRddId() extends SgxExecuteInside[Int] {
	def apply() = Await.result( Future { SgxMain.sparkContext.newRddId() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

//private case class SgxTaskSparkContextRunJob[T, U: ClassTag](
//	rddId: Int,
//    func: (TaskContext, Iterator[T]) => U,
//    partitions: Seq[Int],
//    resultHandler: (Int, U) => Unit) extends SgxExecuteInside[Unit] {
//
//	def apply() = {
//		SgxMain.sparkContext.runJob(SgxMain.rddIds.get(rddId).asInstanceOf[RDD[T]], func, partitions, resultHandler(i,u))
//	}
//
//	override def toString = this.getClass.getSimpleName + "()"
//}

private case class SgxSparkContextStop() extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.sparkContext.stop() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxSparkContextTextFile(path: String) extends SgxExecuteInside[RDD[String]] {
	def apply() = Await.result( Future {
		val rdd = SgxMain.sparkContext.textFile(path)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(path=" + path + ")"
}