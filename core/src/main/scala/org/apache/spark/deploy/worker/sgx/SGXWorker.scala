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

package org.apache.spark.deploy.worker.sgx

import java.io.{DataInputStream, DataOutputStream, IOException}
import java.net.{InetAddress, Socket}
import java.nio.ByteBuffer

import org.apache.spark.api.sgx.{SGXException, SGXFunctionType, SGXRDD, SpecialSGXChars}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkException, TaskContext}

import scala.collection.mutable
import scala.reflect.ClassTag

private[spark] class SGXWorker(closuseSer: SerializerInstance, dataSer: SerializerInstance) extends Logging {
  val SYSTEM_NAME = "SparkSGXWorker"
  val ENDPOINT_NAME = "SecureWorker"
  if (dataSer == null || closuseSer == null) {
    throw new SGXException("Worker Serializer not set", new RuntimeException)
  }

  val shuffleMemoryBytesSpilled: Int = 0
  val shuffleDiskBytesSpilled: Int = 0

  def process(inSock: DataInputStream, outSock: DataOutputStream): Unit = {
    val boot_time = System.nanoTime()

    val split_index = inSock.readInt()
    if (split_index == -1) {
      System.exit(-1)
    }
    val sgx_version = SGXRDD.readUTF(inSock)
    logInfo(s"SGXWorker version ${sgx_version}")
    val boundPort = inSock.readInt()
    val taskContext = TaskContext.get()
    val stageId = inSock.readInt()
    val partitionId = inSock.readInt()
    val attemptId = inSock.readInt()
    val taskAttemptId = inSock.readLong()

    val localProps = new mutable.HashMap[String, String]()
    for (i <- 0 until inSock.readInt()) {
      val k = SGXRDD.readUTF(inSock)
      val v = SGXRDD.readUTF(inSock)
      localProps(k) = v
    }
    val spark_files_dir = SGXRDD.readUTF(inSock)

    // Read Function Type & Function
    val init_time = System.nanoTime()
    val eval_type = inSock.readInt()

    val funcArray: mutable.ArrayBuffer[(Iterator[Any]) => Any] = readFunction(inSock)
    logInfo(s"Executing ${funcArray.size} (pipelined) funcs")

    eval_type match {
      case SGXFunctionType.NON_UDF =>
        // Read Iterator
        val iterator = new ReaderIterator(inSock, dataSer)
        val res = funcArray.head(iterator)
        SGXRDD.writeIteratorToStream[Any](res.asInstanceOf[Iterator[Any]], dataSer, outSock)
        outSock.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        outSock.flush()
      case SGXFunctionType.PIPELINED =>
        val iterator = new ReaderIterator(inSock, dataSer)

        var res: Iterator[Any] = null
        for (func <- funcArray) {
          logDebug(s"Running Func ${func.getClass}")
          res = if (res == null) func(iterator).asInstanceOf[Iterator[Any]] else func(res).asInstanceOf[Iterator[Any]]
        }
        SGXRDD.writeIteratorToStream[Any](res, dataSer, outSock)
        outSock.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        outSock.flush()
      case _ =>
        logError(s"Unsupported FunctionType ${eval_type}")
    }

    val finishTime = System.nanoTime()

    // Write reportTimes AND Shuffle timestamps
    outSock.writeInt(SpecialSGXChars.END_OF_STREAM)
    outSock.flush()
    // send metrics etc
  }

  def reportMetrics(outFile: DataOutputStream, shuffleMemoryBytesSpilled: Int, shuffleDiskBytesSpilled: Int ): Unit = {
    outFile.writeInt(shuffleMemoryBytesSpilled)
    outFile.writeInt(shuffleDiskBytesSpilled)
  }

  def reportTimes(outfile: DataOutputStream, bootTime: Long, initTime: Long, finishTime: Long): Unit = {
    outfile.writeInt(SpecialSGXChars.TIMING_DATA)
    outfile.writeLong(bootTime)
    outfile.writeLong(initTime)
    outfile.writeLong(finishTime)
  }

  def readFunction(inSock: DataInputStream): mutable.ArrayBuffer[(Iterator[Any]) => Any] = {
    val functionArr = mutable.ArrayBuffer[(Iterator[Any]) => Any]()
    var done = false
    while (!done) {
      inSock.readInt() match {
        case func_size if func_size > 0 =>
          val obj = new Array[Byte](func_size)
          inSock.readFully(obj)
          val closure = closuseSer.deserialize[(Iterator[Any]) => Any](ByteBuffer.wrap(obj))
          functionArr.append(closure)
        case SpecialSGXChars.END_OF_FUNC_SECTION =>
          logDebug(s"Read ${functionArr.size} functions Done")
          done = true
      }
    }
    functionArr
  }
}

// Data is encrypted thus Array[Byte] - we should decode them to a type[IN]
private[spark] class ReaderIterator[IN: ClassTag](stream: DataInputStream, dataSer: SerializerInstance) extends Iterator[IN] with Logging {

  private var nextObj: IN = _
  private var eos = false

  override def hasNext: Boolean = nextObj != null || {
    if (!eos) {
      nextObj = read()
      hasNext
    } else {
      false
    }
  }

  override def next(): IN = {
    if (hasNext) {
      val obj = nextObj
      nextObj = null.asInstanceOf[IN]
      logDebug(s"Next is: ${obj}")
      obj
    } else {
      Iterator.empty.next()
    }
  }

  override def size(): Int = {
    throw new SparkException("Not implemented!")
  }

  // FramedSerializer
  /**
    * Reads next object from the stream.
    * When the stream reaches end of data, needs to process the following sections,
    * and then returns null.
    */
  protected def read(): IN = {
    try {
      stream.readInt() match {
        case length if length > 0 =>
          val obj = new Array[Byte](length)
          stream.readFully(obj)
          val elem = dataSer.deserialize[IN](ByteBuffer.wrap(obj))
          return elem.asInstanceOf[IN]
        case SpecialSGXChars.END_OF_DATA_SECTION =>
          eos = true
        case SpecialSGXChars.NULL =>
      }
    } catch {
      case ex: Exception =>
        logError(s"SGXWorker Failed to get Data ${ex}")
    }
    null.asInstanceOf[IN]
  }
}


private[deploy] object SGXWorker extends Logging {

  val conf = new SparkConf(loadDefaults = false)
  var dataSerializer: SerializerInstance = null
  // should always use JavaSerializer for closures
  val closureSerializer = new JavaSerializer(null).newInstance()

  def localConnect(port: Int): Socket = {
    try {
      val ia = InetAddress.getByName("localhost")
      val socket = new Socket(ia, port)
      socket
    } catch {
      case e: IOException =>
        logError(s"Could not open socket on port:${port}")
        null
    }
  }

  def instantiateClass[T](className: String): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, new java.lang.Boolean(false))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  def main(args: Array[String]): Unit = {
    Utils.initDaemon(log)
    val workerDebugEnabled = sys.env("SGX_WORKER_DEBUG").toBoolean
    val workerSerializer = sys.env("SGX_WORKER_SERIALIZER")
    dataSerializer = SGXWorker.instantiateClass[Serializer](workerSerializer).newInstance()

    val worker = new SGXWorker(closureSerializer, dataSerializer)
    val socket = localConnect(if (workerDebugEnabled) 65000 else sys.env("SGX_WORKER_FACTORY_PORT").toInt)
    val outStream = new DataOutputStream(socket.getOutputStream())
    val inStream = new DataInputStream(socket.getInputStream())

    worker.process(inStream, outStream)
  }

}
