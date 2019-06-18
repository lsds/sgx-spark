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

package org.apache.spark.rdd

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.spark._
import org.apache.spark.api.python.PythonFunction
import org.apache.spark.api.sgx.{SGXEvalType, SGXRDD, SpecialChars}
import org.apache.spark.deploy.worker.sgx.{ReaderIterator, SGXWorker}
import org.apache.spark.util.Utils


class RDDSuiteSGX extends SparkFunSuite {
  var tempDir: File = _
  var conf : SparkConf = _
  var sc : SparkContext = _

  override def beforeAll(): Unit = {
    tempDir = Utils.createTempDir()
    conf = new SparkConf().setMaster("local").setAppName("RDD SGX suite test")
    conf.enableSparkSGX()
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  test("basic SGX operations") {
    val nums = sc.makeRDD(Array("1", "2", "3", "4"), 1)
    assert(nums.getNumPartitions === 1)
    println(nums.count())
//    val pRdd = new SGXRDD(nums, null, false, false)
//    pRdd.compute(nums.partitions(0), TaskContext.get())
//    assert(pRdd.collect().toList === List(1, 2, 3, 4))
  }

  test("SGX Iterator Reader test") {
    val baos = new ByteArrayOutputStream
    val dos = new DataOutputStream(baos)
    SGXRDD.writeIteratorToStream(Iterator("a", "b", "c"), dos)
    dos.writeInt(SpecialChars.END_OF_DATA_SECTION)

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val dis = new DataInputStream(bais)

    val it = new ReaderIterator(dis)
    var count = 0
    val expected_val = List("a", "b", "c")
    while (it.hasNext) {
      assert(expected_val(count) == new String(it.next(), StandardCharsets.UTF_8))
      count += 1
    }
  }

  test("SGX socket timing test: strings") {
    val baos = new ByteArrayOutputStream
    val dos = new DataOutputStream(baos)

    val itemCount = 999999
    val input: List[String] = List.tabulate(itemCount)(n => "Here's a new string, count: " + n)

    var receivedCount = 0
    time {
      SGXRDD.writeIteratorToStream(input.iterator, dos)
      dos.writeInt(SpecialChars.END_OF_DATA_SECTION)

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val dis = new DataInputStream(bais)

      val it = new ReaderIterator(dis)
      while (it.hasNext) {
        val next = it.next()
        receivedCount += 1
      }
    }
    assert(itemCount == receivedCount)
  }

  // Helper function to time the execution of a given block
  def time[R](blockToTime: => R): R = {
    val t0 = System.nanoTime()
    val result = blockToTime
    val t1 = System.nanoTime()
    val duration = (t1 - t0) / 1e9d
    println("Socket timing test elapsed: " + duration + " (seconds)");
    result
  }

  val test_func = (it: Iterator[String]) => {
    var sum = ""
    while (it.hasNext) {
      sum += it.next()
    }
    Array("Success").toIterator
  }

  test("SGXWorker write/read process test") {
    val baos = new ByteArrayOutputStream
    val dos = new DataOutputStream(baos)

    // Partition index
    dos.writeInt(1)
    SGXRDD.writeUTF("999", dos)
    // port
    dos.writeInt(65500)
    // stageId
    dos.writeInt(0)
    // partitionId
    dos.writeInt(20)
    // attemptNumber
    dos.writeInt(0)
    // taskAttemptId
    dos.writeLong(1)
    val localPros = new mutable.HashMap[String, String]()
    localPros.put("testKey", "testValue")
    dos.writeInt(localPros.size)
    localPros.foreach { case (k, v) =>
      SGXRDD.writeUTF(k, dos)
      SGXRDD.writeUTF(v, dos)
    }

    SGXRDD.writeUTF(SparkFiles.getRootDirectory(), dos)
    dos.flush()
    dos.writeInt(SGXEvalType.NON_UDF)
    // Func serialize
    val command = SparkEnv.get.closureSerializer.newInstance().serialize(test_func)
    dos.writeInt(command.array().size)
    dos.write(command.array())
    // Data serialize
    SGXRDD.writeIteratorToStream(Iterator("1", "2", "3"), dos)
    dos.writeInt(SpecialChars.END_OF_DATA_SECTION)

    dos.flush()

    val worker = new SGXWorker()
    // Convert bytestream to input
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val dis = new DataInputStream(bais)


    val baosIn = new ByteArrayOutputStream
    val dosIn = new DataOutputStream(baosIn)

    worker.process(dis, dosIn)

    val baisIn = new ByteArrayInputStream(baosIn.toByteArray)
    val disIn = new DataInputStream(baisIn)

    val itIn = new ReaderIterator(disIn)
    println(new String(itIn.next(), StandardCharsets.UTF_8))
  }


  // This Python UDF is dummy and just for testing. Unable to execute.
  class DummyUDF extends PythonFunction(
    command = Array[Byte](),
    envVars = Map("" -> "").asJava,
    pythonIncludes = ArrayBuffer("").asJava,
    pythonExec = "",
    pythonVer = "",
    broadcastVars = null,
    accumulator = null)
}
