package org.apache.spark.mllib.clustering.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark.sgx.SgxMessage
import org.apache.spark.mllib.clustering.{VectorWithNorm, SgxVectorWithNorm}
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.internal.Logging

case class SgxTaskVectorsToDense(v: SgxVectorWithNorm) extends SgxMessage[VectorWithNorm] with Logging {

	def execute() = Await.result( Future { v.decrypt[VectorWithNorm].toDense }, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(v=" + v + ")"
}
