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
          case (x, y) => {
            x + y
          }
        }

      counts.collect
    }

    logInfo("Result:")
    logInfo("" + result.mkString(", "))

    //    .collect()
    //    .foreach(x => logDebug(x.toString()))

    // Save the word count back out to a text file, causing evaluation.
    // counts.saveAsTextFile(outputFile)
  }
}
