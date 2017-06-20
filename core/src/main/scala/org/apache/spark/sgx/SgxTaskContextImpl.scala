package org.apache.spark.sgx

import org.apache.spark.TaskContext

//class SgxTaskContextImpl(val tc: TaskContext) extends TaskContext {
//	private val taskContext: TaskContext = tc
//	
//	override def isCompleted(): Boolean = tc.isCompleted()
//
//	override def isRunningLocally(): Boolean = tc.isRunningLocally()
//	
//	override def isInterrupted(): Boolean = tc.isInterrupted()
//
//	override def getMetricsSources(sourceName: String): Seq[Source] = tc.getMetricsSources(sourceName)
//
//	private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = tc.registerAccumulator(a)
//
//	private[spark] override def setFetchFailed(fetchFailed: FetchFailedException): Unit = tc.setFetchFailed(fetchFailed)
//}