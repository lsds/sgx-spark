package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object LineCount extends Logging {

  def time[R](block: => R): R = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + java.text.NumberFormat.getIntegerInstance.format((t1 - t0)/1000000) + "ms")
    logInfo("Elapsed time: " + java.text.NumberFormat.getIntegerInstance.format((t1 - t0)/1000000) + "ms")
    result
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("lineCount")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    val r = time {
      sc.textFile(inputFile).map(x => x + "x").map(x => x + x).map(x => "foo" + x + "bar").count()
    }

    // Load our input data.
    logInfo("lines: " +  r)
  }
}
