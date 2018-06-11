package org.apache.spark.sgx

import java.util.concurrent.Callable

trait SgxCallable[T] extends Callable[T] {
  private var running = true
  
  def isRunning = running
  
  def stop = {
    running = false
  }
}