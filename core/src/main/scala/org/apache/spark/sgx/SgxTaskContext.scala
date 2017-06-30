package org.apache.spark.sgx

import org.apache.spark.TaskContext
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.TaskFailureListener
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source

class SgxTaskContext(context: TaskContext, port: Int) extends TaskContext {
	def getPort(): Int = { port }

  override def isCompleted() = context.isCompleted()
  override def isInterrupted() = context.isInterrupted()
  override def addTaskCompletionListener(listener: TaskCompletionListener) = context.addTaskCompletionListener(listener)
  override def addTaskCompletionListener(f: (TaskContext) => Unit) = context.addTaskCompletionListener(f)
  override def addTaskFailureListener(listener: TaskFailureListener) = context.addTaskFailureListener(listener)
  override def addTaskFailureListener(f: (TaskContext, Throwable) => Unit) = context.addTaskFailureListener(f)
  override def stageId() = context.stageId()
  override def partitionId() = context.partitionId()
  override def attemptNumber() = context.attemptNumber()
  override def taskAttemptId() = context.taskAttemptId()
  override def getLocalProperty(key: String) = context.getLocalProperty(key)
  override def taskMetrics() = context.taskMetrics()
  override def getMetricsSources(sourceName: String) = context.getMetricsSources(sourceName)
  override def isRunningLocally() = context.isRunningLocally()

  private[spark] def killTaskIfInterrupted() = context.killTaskIfInterrupted()
  private[spark] def getKillReason() = context.getKillReason()
  private[spark] def taskMemoryManager() = context.taskMemoryManager()
  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]) = context.registerAccumulator(a)
  private[spark] def setFetchFailed(fetchFailed: FetchFailedException) = context.setFetchFailed(fetchFailed)

}