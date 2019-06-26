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

import org.apache.spark._

import scala.collection.mutable.ArrayBuffer

private[spark] object SGXRunner {
  def apply(func: (Iterator[Any]) => Any): SGXRunner = {
    new SGXRunner(func, SGXFunctionType.NON_UDF, ArrayBuffer.empty)
  }

  def apply(func: (Iterator[Any]) => Any, funcType: Int): SGXRunner = {
    new SGXRunner(func, funcType, ArrayBuffer.empty)
  }

  def apply(func: (Iterator[Any]) => Any, funcType: Int, funcs: ArrayBuffer[(Iterator[Any]) => Any]): SGXRunner = {
    new SGXRunner(func, funcType, funcs)
  }
}

/** Helper class to run a function in SGX Spark */
private[spark] class SGXRunner(func: (Iterator[Any]) => Any, funcType: Int, funcs: ArrayBuffer[(Iterator[Any]) => Any])
  extends SGXBaseRunner[Array[Byte], Array[Byte]](func, funcType, funcs) {

  override protected def sgxWriterThread(env: SparkEnv,
                                         worker: Socket,
                                         inputIterator: Iterator[Array[Byte]],
                                         partitionIndex: Int, context: TaskContext): WriterIterator = {
    new WriterIterator(env, worker, inputIterator, partitionIndex, context) {
      /** Writes a command section to the stream connected to the SGX worker */
      override protected def writeFunction(dataOut: DataOutputStream): Unit = {
        logInfo(s"Ser ${funcs.size + 1} closures")
        for (currFunc <- funcs) {
          logDebug(s"Ser closure: ${currFunc.getClass}")
          val command = closureSer.serialize(currFunc)
          dataOut.writeInt(command.array().size)
          dataOut.write(command.array())
        }
        val command = closureSer.serialize(func)
        logDebug(s"Ser func: ${func.getClass}")
        dataOut.writeInt(command.array().size)
        dataOut.write(command.array())
        dataOut.writeInt(SpecialSGXChars.END_OF_FUNC_SECTION)
        dataOut.flush()
      }

      /** Writes input data to the stream connected to the SGX worker */
      override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        SGXRDD.writeIteratorToStream(inputIterator, iteratorSer, dataOut)
        dataOut.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
      }
    }
  }
}

