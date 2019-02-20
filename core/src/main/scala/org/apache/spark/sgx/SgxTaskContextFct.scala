package org.apache.spark.sgx

import org.apache.spark.{TaskContext}
import org.apache.spark.util.ThreadUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object SgxTaskContextFct {
  private var taskContext: TaskContext = null

  def getTaskContext = {
    if (taskContext == null) taskContext = GetTaskContext().send()
    taskContext
  }

}

private case class GetTaskContext() extends SgxMessage[TaskContext] {
  def execute() = ThreadUtils.awaitResult( Future { TaskContext.get() }, Duration.Inf)
}