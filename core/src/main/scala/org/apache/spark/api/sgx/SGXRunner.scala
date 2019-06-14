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

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark._

private[spark] object SGXRunner {
  def apply(func: (Iterator[Any]) => Any): SGXRunner = {
    new SGXRunner(func)
  }
}

/** Helper class to run a function in SGX Spark */
private[spark] class SGXRunner(func: (Iterator[Any]) => Any) extends
  SGXBaseRunner[Array[Byte], Array[Byte]](func, SGXEvalType.NON_UDF) {

  override protected def sgxWriterThread(env: SparkEnv,
                                         worker: Socket,
                                         inputIterator: Iterator[Array[Byte]],
                                         partitionIndex: Int, context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {
      /** Writes a command section to the stream connected to the SGX worker */
      override protected def writeFunction(dataOut: DataOutputStream): Unit = {
        // TODO Panos: check if we can avoid serialization here
        val command = SparkEnv.get.closureSerializer.newInstance().serialize(func)
        println(command.array().size)
        dataOut.writeInt(command.array().size)
        dataOut.write(command.array())
        dataOut.flush()
      }

      /** Writes input data to the stream connected to the SGX worker */
      override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        SGXRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialChars.END_OF_DATA_SECTION)
      }
    }

  }

  override protected def sgxReaderIterator(stream: DataInputStream,
                                           writerThread: WriterThread,
                                           startTime: Long,
                                           env: SparkEnv,
                                           worker: Socket,
                                           releasedOrClosed: AtomicBoolean,
                                           context: TaskContext): Iterator[Array[Byte]] = {
    new ReaderIterator(stream, writerThread, startTime, env, worker, releasedOrClosed, context) {

      protected override def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj

            case SpecialChars.EMPTY_DATA =>
              Array.empty[Byte]
            case SpecialChars.TIMING_DATA =>
              handleTimingData()
              read()
            case SpecialChars.SGX_EXCEPTION_THROWN =>
              throw handleSGXException()
            case SpecialChars.END_OF_DATA_SECTION =>
//              TODO PANOs: Send stats
              handleEndOfDataSection()
              null
          }
        } catch handleException
      }
    }
  }
}

private[spark] object SpecialChars {
  val EMPTY_DATA = 0
  val END_OF_DATA_SECTION = -1
  val SGX_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
  val START_ARROW_STREAM = -6
}
