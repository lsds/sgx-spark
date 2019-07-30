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
import java.io._

import jocket.net.ServerJocket
import org.apache.spark._
import org.apache.spark.api.sgx.{SGXFunctionType, SGXRDD, SpecialSGXChars}
import org.apache.spark.deploy.worker.sgx.{ReaderIterator, SGXWorker}
import org.apache.spark.util.{SGXUtils, Utils}


class RDDSuiteSGX extends SparkFunSuite {
  var tempDir: File = _
  var conf : SparkConf = _
  var sc : SparkContext = _

  override def beforeAll(): Unit = {
    tempDir = Utils.createTempDir()
    conf = new SparkConf().setMaster("local").setAppName("RDD SGX suite test")
    conf.enableSGXWorker()
//    conf.enableSGXDebug()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
    val nums = sc.makeRDD(Array("1", "2", "3", "4"), 2)
    assert(nums.getNumPartitions === 2)
    val res = nums.count()
    assert(res == 4)
  }

  ignore("SGX mapPartitionsWithIndex") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.getNumPartitions === 2)
    val partitionSumsWithSplit = nums.mapPartitionsWithIndex(SGXUtils.mapPartitionsWithIndex)
    assert(partitionSumsWithSplit.collect().toList === List((0, 3), (1, 7)))
  }

  test("SGX pipelined operations") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.getNumPartitions === 2)
    val res = nums.
      map(SGXUtils.mapIncrementOneFunc).
      filter(SGXUtils.filterEvenNumFunc).
      collect()
    assert(res.size == 2)
    assert(res(0) == 2)
    assert(res(1) == 4)
  }

  test("SGX mapPartitions operations") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.getNumPartitions === 2)
    val partitionSums = nums.mapPartitions(SGXUtils.mapPartitionsSum)
    assert(partitionSums.collect().toList === List(3, 7))
  }

  test("SGX flatMap operations") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 1)
    assert(nums.getNumPartitions === 1)
    assert(nums.flatMap(SGXUtils.flatMapOneToVal).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
  }

  test("SGX BypassMergeSort shuffle operation") {
    val kvPairs = sc.parallelize(Array(
      ("USA", 1), ("USA", 2), ("UK", 6), ("UK", 9),
      ("India", 4), ("India", 1), ("USA", 8), ("USA", 3),
      ("UK", 5), ("UK", 1), ("India", 4), ("India", 9)
    ), 2)
    val res = kvPairs.groupByKey().map(SGXUtils.groupBySum)
    val resK = res.collect
    assert(resK.size == 3)
  }

  test("SGX Iterator Reader test") {
    val baos = new ByteArrayOutputStream
    val dos = new DataOutputStream(baos)
    val iteratorSerializer = SparkEnv.get.serializer.newInstance()
    SGXRDD.writeIteratorToStream(Iterator("a", "b", "c"), iteratorSerializer, dos)
    dos.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val dis = new DataInputStream(bais)

    val it = new ReaderIterator[String](dis, iteratorSerializer)
    var count = 0
    val expected_val = List("a", "b", "c")
    while (it.hasNext) {
      val elem = it.next()
      assert(elem.getClass == "".getClass)
      assert(expected_val(count) == elem)
      count += 1
    }
  }

  test("SGX Jocket Test") {
    // server
    val srv = new ServerJocket(4242)
    assert(srv.getLocalPort == 4242)
  }

  test("SGX socket timing test: clone objects") {
    val itemCount = 999999
    // Tuple is: (NAME, AGE, MALE?)
    val input: List[Tuple3[String, Int, Boolean]] = List.tabulate(itemCount)(n => new Tuple3("Human: " + n, n, true))

    // Can be Java/Kryo/Avro etc.
    val iteratorSerializer = SparkEnv.get.serializer.newInstance()

    val receivedCount = writeToAndReadFromStream(itemCount, input, iteratorSerializer)
    assert(itemCount == receivedCount)
  }

  test("SGX socket timing test: ints") {
    val itemCount = 999999
    val input: List[Int] = List.tabulate(itemCount)(n => n)

    // Can be Java/Kryo/Avro etc.
    val iteratorSerializer = SparkEnv.get.serializer.newInstance()

    val receivedCount = writeToAndReadFromStream(itemCount, input, iteratorSerializer)
    assert(itemCount == receivedCount)
  }

  test("SGX socket timing test: strings") {
    val itemCount = 999999
    val input: List[String] = List.tabulate(itemCount)(n => "Here's a new string, count: " + n)

    // Can be Java/Kryo/Avro etc.
    val iteratorSerializer = SparkEnv.get.serializer.newInstance()

    val receivedCount = writeToAndReadFromStream(itemCount, input, iteratorSerializer)
    assert(itemCount == receivedCount)
  }

  // Helper method to write items to stream, and read items from stream using serializer
  def writeToAndReadFromStream(itemCount: Int, input: List[Any],
                               iteratorSerializer: org.apache.spark.serializer.SerializerInstance) : Int = {
    val baos = new ByteArrayOutputStream
    val dos = new DataOutputStream(baos)

    var receivedCount = 0
    time {
      SGXRDD.writeIteratorToStream(input.iterator, iteratorSerializer, dos)
      dos.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)

      val bais = new ByteArrayInputStream(baos.toByteArray)
      val dis = new DataInputStream(bais)

      val it = new ReaderIterator[Any](dis, iteratorSerializer)
      while (it.hasNext) {
        val next = it.next()
        receivedCount += 1
      }
    }

    receivedCount
  }

  // Helper function to time the execution of a given block
  def time[R](blockToTime: => R): R = {
    val t0 = System.nanoTime()
    val result = blockToTime
    val t1 = System.nanoTime()
    val duration = (t1 - t0) / 1e9d
    println("Time elapsed: " + duration + " (seconds)");
    result
  }

  val test_func = (it: Iterator[String]) => {
    var sum = 0
    while (it.hasNext) {
      sum += 1
      it.next()
    }
    Array(sum).toIterator
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

    val iteratorSerializer = SparkEnv.get.serializer.newInstance()

    SGXRDD.writeUTF(SparkFiles.getRootDirectory(), dos)
    dos.flush()

    dos.writeInt(SGXFunctionType.NON_UDF)
    // Func serialize
    val command = SparkEnv.get.closureSerializer.newInstance().serialize(test_func)
    dos.writeInt(command.array().size)
    dos.write(command.array())
    // Data serialize
    SGXRDD.writeIteratorToStream(Iterator("1", "2", "3"), iteratorSerializer, dos)
    dos.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
    dos.flush()

    val worker = new SGXWorker(SparkEnv.get.closureSerializer.newInstance(), SparkEnv.get.serializer.newInstance())
    // Convert bytestream to input
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val dis = new DataInputStream(bais)

    val baosIn = new ByteArrayOutputStream
    val dosIn = new DataOutputStream(baosIn)

    worker.process(dis, dosIn)

    val baisIn = new ByteArrayInputStream(baosIn.toByteArray)
    val disIn = new DataInputStream(baisIn)

    val itIn = new ReaderIterator[Any](disIn, iteratorSerializer)
    while(itIn.hasNext) {
      val v = itIn.next()
      assert(v == 3)
    }
  }
}
