package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.util.AccumulatorMetadata

object SgxAccumulatorV2Fct {
	def register(acc: AccumulatorV2[_, _], name: Option[String] = None) = new SgxTaskAccumulatorRegister(acc, name).send()
}

private case class SgxTaskAccumulatorRegister[T,U](
		acc: AccumulatorV2[T,U],
		name: Option[String]) extends SgxMessage[AccumulatorMetadata] {

	def execute() = {
		if (name == null) acc.register(SgxMain.sparkContext)
		else acc.register(SgxMain.sparkContext, name)
		acc.metadata
	}

	override def toString = this.getClass.getSimpleName + "()"
}

