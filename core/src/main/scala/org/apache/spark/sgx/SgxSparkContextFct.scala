package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object SgxSparkContextFct {

	def broadcast[T: ClassTag](value: T) = new SgxTaskSparkContextBroadcast(value).executeInsideEnclave()

	def conf() = new SgxTaskSparkContextConf().executeInsideEnclave()

	def defaultParallelism() = new SgxTaskSparkContextDefaultParallelism().executeInsideEnclave()

	def newRddId() = new SgxTaskSparkContextNewRddId().executeInsideEnclave()

//	def runJob[T, U: ClassTag](
//		rddId: Int,
//    	func: (TaskContext, Iterator[T]) => U,
//    	partitions: Seq[Int],
//    	resultHandler: (Int, U) => Unit) = new SgxTaskSparkContextRunJob(rddId, func, partitions, resultHandler).executeInsideEnclave()

	def stop() = new SgxTaskSparkContextStop().executeInsideEnclave()

	def textFile(path: String) = new SgxTaskSparkContextTextFile(path).executeInsideEnclave()
}

private case class SgxTaskSparkContextBroadcast[T: ClassTag](value: T) extends SgxExecuteInside[Broadcast[T]] {
	def apply() = Await.result( Future { SgxMain.sparkContext.broadcast(value) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxTaskSparkContextConf() extends SgxExecuteInside[SparkConf] {
	def apply() = Await.result( Future { SgxMain.sparkContext.conf }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxTaskSparkContextDefaultParallelism() extends SgxExecuteInside[Int] {
	def apply() = Await.result( Future { SgxMain.sparkContext.defaultParallelism }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxTaskSparkContextNewRddId() extends SgxExecuteInside[Int] {
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

private case class SgxTaskSparkContextStop() extends SgxExecuteInside[Unit] {
	def apply() = Await.result( Future { SgxMain.sparkContext.stop() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxTaskSparkContextTextFile(path: String) extends SgxExecuteInside[RDD[String]] {
	def apply() = Await.result( Future {
		val rdd = SgxMain.sparkContext.textFile(path)
		SgxMain.rddIds.put(rdd.id, rdd)
		rdd
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(path=" + path + ")"
}