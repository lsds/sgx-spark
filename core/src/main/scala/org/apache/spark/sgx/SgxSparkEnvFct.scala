package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.Serializer

object SgxSparkEnvFct {

	def getSerializer = GetSerializer().send()
}

private case class GetSerializer() extends SgxMessage[Serializer] {
	def execute() = Await.result( Future { SparkEnv.get.serializer }, Duration.Inf)
}
