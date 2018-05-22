package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.internal.Logging

object MyZipPartitionExample extends Logging {
  def myfunc(aiter: Iterator[String], biter: Iterator[Int]): Iterator[(String, Int)] =
  {
    var res = List[(String, Int)]()
    while (aiter.hasNext && biter.hasNext)
    {
      val x = (aiter.next, biter.next)
      res ::= x
    }
    res.iterator
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.
    val input = sc.textFile(inputFile)

    println("Data has been loaded");

    // Split up into words.
    val words = input.flatMap(line => {
      line.split(" ")
    })

    println("words have been obtained");

    // Map words to their length
    val lengths = words.map(word => {
      word.length
    })

    println("Lengths have been obtained")

    // do a zip
    words.zipPartitions(lengths, preservesPartitioning = true)(myfunc)
      .collect()
      .foreach(x => logDebug(x.toString()))

    println("zipPartitions executed!");
  }
}
