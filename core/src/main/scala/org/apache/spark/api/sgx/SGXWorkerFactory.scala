/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.api.sgx

import java.io.InputStream
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.Arrays

import scala.collection.JavaConverters._

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RedirectThread

import jocket.net.ServerJocket

private[spark] class SGXWorkerFactory(envVars: Map[String, String])
  extends Logging {

  val sgxWorkerModule = "org.apache.spark.deploy.worker.sgx.SGXWorker"
  val sgxWorkerExec = s"${System.getenv("SPARK_HOME")}/sbin/start-sgx-slave.sh"


  private def createSimpleSGXWorker(): Socket = {
    var serverSocket: ServerSocket = null
    val workerDebug = SparkEnv.get.conf.isSGXDebugEnabled()
    val serverSocketPort = if (workerDebug) 65000 else 0
    try {
      val enableJocket = SparkEnv.get.conf.isJocketEnabled()
      if (enableJocket) {
        serverSocket = new ServerJocket(serverSocketPort)
      } else {
        serverSocket = new ServerSocket(serverSocketPort, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))
      }

      // Create and start the worker
      val pb = new ProcessBuilder(Arrays.asList(sgxWorkerExec, sgxWorkerModule))

      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)

      logInfo(s"Unsecure worker port: ${serverSocket.getLocalPort.toString}")
      workerEnv.put("SGX_WORKER_FACTORY_PORT", serverSocket.getLocalPort.toString)
      workerEnv.put("SGX_WORKER_SERIALIZER", SparkEnv.get.conf.getOption("spark.serializer").
        getOrElse("org.apache.spark.serializer.JavaSerializer"))
      workerEnv.put("SGX_WORKER_DEBUG", workerDebug.toString)
      workerEnv.put("SGX_WORKER_JOCKET", enableJocket.toString)
      // TODO PANOS: Keep track of running workers
      if (!workerDebug) {
        val worker = pb.start()
        // Redirect worker stdout and stderr
        redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)
      }
      // else connect manually

      // Wait for worker to connect to our socket
      serverSocket.setSoTimeout(if (!workerDebug) 100000 else 1000000)

      try {
        val socket = serverSocket.accept()
        log.info(s"SGXWorker successfully connected at Port:${serverSocket.getLocalPort}")
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("SGXWorker worker failed to connect back.", e)
      }

    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

  def create(): Socket = {
    // TODO: Panos Avoid starting a new worker every time - use a Daemon instead?
    createSimpleSGXWorker()
  }

  /**
    * Redirect the given streams to our stderr in separate threads.
    */
  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for " + sgxWorkerExec).start()
      new RedirectThread(stderr, System.err, "stderr reader for " + sgxWorkerExec).start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }
}
