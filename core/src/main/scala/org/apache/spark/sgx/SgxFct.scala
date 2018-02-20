package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark.Partitioner
import org.apache.spark.util.collection.AppendOnlyMap
import org.apache.spark.util.collection.AppendOnlyMapIdentifier

object SgxFct {
	def externalSorterInsertAllCreateKey[K](partitioner: Partitioner, pair: Product2[K,Any]) =
		new ExternalSorterInsertAllCreateKey[K](partitioner, pair).send()

	def appendOnlyMapCreate[K,V]() =
		new AppendOnlyMapCreate().send()

	def fct0[Z](fct: () => Z) = new SgxFct0[Z](fct).send()

	def fct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) = new SgxFct2[A, B, Z](fct, a, b).send()
}

private case class ExternalSorterInsertAllCreateKey[K](
	partitioner: Partitioner,
	pair: Product2[K,Any]) extends SgxMessage[(Int,K)] {

	def execute() = Await.result( Future {
		(partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1), pair._1)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
}

private case class AppendOnlyMapCreate[K,V]() extends SgxMessage[AppendOnlyMapIdentifier] {

	def execute() = Await.result( Future {
		logDebug("AppendOnlyMapCreate")
		new AppendOnlyMap().id
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "()"
}

private case class SgxFct0[Z](fct: () => Z) extends SgxMessage[Z] {
	def execute() = Await.result(Future { fct() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "))"
}

private case class SgxFct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) extends SgxMessage[Z] {
	def execute() = Await.result(Future { fct(a, b) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}
