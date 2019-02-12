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

package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object MyWordCount extends Logging {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logInfo("Elapsed time: " + java.text.NumberFormat.getIntegerInstance.format((t1 - t0)/1000000) + "ms")
    result
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf()

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.    

    val result = time {
      val input = sc.textFile(inputFile)

      // Split up into words.
      val words = input.flatMap(line => {
        line.split(" ")
      })

      // Transform into word and count.
      val counts = words.map(word => {
        (word, 1)
      })
      .reduceByKey {
        case (x, y) => x + y
      }.collect
    }

    logInfo("Result:")
    logInfo("" + result.mkString(", "))

    //    .collect()
    //    .foreach(x => logDebug(x.toString()))

    // Save the word count back out to a text file, causing evaluation.
    // counts.saveAsTextFile(outputFile)
  }
}
