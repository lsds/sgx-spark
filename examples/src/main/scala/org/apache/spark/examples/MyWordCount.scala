package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.internal.Logging

object MyWordCount extends Logging {
  def main(args: Array[String]) {
	  logDebug("example 0")
    var inputFile: String = null
    try {
    inputFile = args(0)
    } catch {
    	case e: Exception => logDebug("example 1 " + e.getMessage + " " + e.getCause + " " + e.getStackTraceString)
    }
    logDebug("example 1a " + inputFile)
    var outputFile: String = null
    try {
    outputFile = args(1)
    } catch {
    	case e: Exception => logDebug("example 2 " + e.getMessage + " " + e.getCause + " " + e.getStackTraceString)
    }
    logDebug("example 2a: " + outputFile)
    val conf = new SparkConf().setAppName("wordCount")
    logDebug("example 3")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    logDebug("example 4")
    // Load our input data.
    val input = sc.textFile(inputFile)
    logDebug("example 5")
    // Split up into words.
    val words = input.flatMap(line => {
    	logDebug("example 6")
      line.split(" ")
    })
    logDebug("example 7")
    // Transform into word and count.
    val counts = words.map(word => {
    	logDebug("example 8")
      (word, 1)
    })
    .reduceByKey {
      case (x, y) => {
    	logDebug("example 9")
        x + y
      }
    }
    logDebug("example 10")
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
    logDebug("example 11")
  }
}
