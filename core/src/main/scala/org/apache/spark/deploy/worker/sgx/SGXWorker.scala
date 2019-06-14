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

import org.apache.spark.api.sgx.{SGXRDD, SpecialChars}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}

import scala.collection.mutable

private[spark] class SGXWorker extends Logging {
  val SYSTEM_NAME = "sparkSGXWorker"
  val ENDPOINT_NAME = "SGXWorker"

  val shuffleMemoryBytesSpilled: Int = 0
  val shuffleDiskBytesSpilled: Int = 0

  val serializer = new JavaSerializer(null)
  val instance = serializer.newInstance()

  def process(inSock: DataInputStream, outSock: DataOutputStream): Unit = {
    val boot_time = System.nanoTime()

    val split_index = inSock.readInt()
    if (split_index == -1) {
      System.exit(-1)
    }
    val sgx_version = SGXRDD.readUTF(inSock)
    println(s"Current SGX version ${sgx_version}")
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

    println(localProps.toList.toString())

    val spark_files_dir = SGXRDD.readUTF(inSock)
    val eval_type = inSock.readInt()

    println(eval_type)

    val func = readFunction(inSock)
//    if (eval_type == SGXEvalType.NON_UDF) {
//      logInfo("We are getting somewhere")
//    }

    val init_time = System.nanoTime()
    //    iterator = deserializer.load_stream(infile)
    //    serializer.dump_stream(func(split_index, iterator), outfile)
    val iterator = new ReaderIterator(inSock)

    val res = func(iterator)
    println("Do miracles happen??")

    SGXRDD.writeIteratorToStream(res.asInstanceOf[Iterator[Any]], outSock)
    outSock.writeInt(SpecialChars.END_OF_DATA_SECTION)
    outSock.flush()

    val finishTime = System.nanoTime()

    outSock.writeInt(SpecialChars.END_OF_STREAM)
    outSock.flush()
    // send metrics etc
  }

  def reportMetrics(outFile: DataOutputStream, shuffleMemoryBytesSpilled: Int, shuffleDiskBytesSpilled: Int ): Unit = {
    outFile.writeInt(shuffleMemoryBytesSpilled)
    outFile.writeInt(shuffleDiskBytesSpilled)
  }

  def reportTimes(outfile: DataOutputStream, bootTime: Long, initTime: Long, finishTime: Long): Unit = {
    outfile.writeInt(SpecialChars.TIMING_DATA)
    outfile.writeLong(bootTime)
    outfile.writeLong(initTime)
    outfile.writeLong(finishTime)
  }

  def readFunction(in_sock: DataInputStream): (Iterator[Any]) => Any = {
    val func_size = in_sock.readInt()
    val obj = new Array[Byte](func_size)
    in_sock.readFully(obj)
    instance.deserialize[(Iterator[Any]) => Any](ByteBuffer.wrap(obj))
//    SparkEnv.get.closureSerializer.newInstance().deserialize[(Iterator[Any]) => Any](ByteBuffer.wrap(obj))
  }
}

private[spark] class ReaderIterator(stream: DataInputStream) extends Iterator[Array[Byte]] with Logging {

  private var nextObj: Array[Byte] = _
  private var eos = false

  override def hasNext: Boolean = nextObj != null || {
    if (!eos) {
      nextObj = read()
      hasNext
    } else {
      false
    }
  }

  override def next(): Array[Byte] = {
    if (hasNext) {
      val obj = nextObj
      nextObj = null.asInstanceOf[Array[Byte]]
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
  protected def read(): Array[Byte] = {
    try {
      stream.readInt() match {
        case length if length > 0 =>
          val obj = new Array[Byte](length)
          stream.readFully(obj)
          return obj
        case SpecialChars.END_OF_DATA_SECTION =>
          eos = true
          Array.empty[Byte]
        case SpecialChars.NULL =>
          Array.empty[Byte]
      }
    } catch {
      case ex: Exception =>
        logError(s"Failed to get Data ${ex}")
    }
    null
  }
}


private[deploy] object SGXWorker extends Logging {
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

  def main(args: Array[String]): Unit = {
    val port_argument = sys.env("SGX_WORKER_FACTORY_PORT").toInt
    val worker = new SGXWorker
    val socket = localConnect(port_argument)
    val outStream = new DataOutputStream(socket.getOutputStream())
    val inStream = new DataInputStream(socket.getInputStream())

    worker.process(inStream, outStream)
  }

}
