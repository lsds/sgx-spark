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

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SpecialLengths
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.SerializerInstance

private[spark] class SGXRDD(parent: RDD[_],
                            func: (Iterator[Any]) => Any,
                            preservePartitoning: Boolean,
                            isFromBarrier: Boolean = false)
  extends RDD[Array[Byte]](parent) {

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitoning) firstParent.partitioner else None
  }

  val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val runner = SGXRunner(func)
    runner.compute(firstParent.iterator(split, context), split.index, context)
  }

}

/** Thrown for exceptions in user Python code. */
private[spark] class SGXException(msg: String, cause: Exception)
  extends RuntimeException(msg, cause)

private[spark] object SGXRDD extends Logging {

  // remember the broadcasts sent to each worker
  private val workerBroadcasts = new mutable.WeakHashMap[Socket, mutable.Set[Long]]()

  def writeIteratorToStream[T](iter: Iterator[T], serializer: SerializerInstance, dataOut: DataOutputStream) {
    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case _ =>
        val outSerArray = serializer.serialize(obj).array()
        dataOut.writeInt(outSerArray.length)
        logDebug(s"SGX => Writing: ${outSerArray}")
        dataOut.write(outSerArray)
        // throw new SparkException("Unexpected element type " + other.getClass)
    }
    iter.foreach(write)
  }

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  def readUTF(dataIn: DataInputStream): String = {
    val strLen = dataIn.readInt()
    val strBytes = new Array[Byte](strLen)
    dataIn.read(strBytes)
    new String(strBytes, StandardCharsets.UTF_8)
  }

  def getWorkerBroadcasts(worker: Socket): mutable.Set[Long] = {
    synchronized {
      workerBroadcasts.getOrElseUpdate(worker, new mutable.HashSet[Long]())
    }
  }
}
