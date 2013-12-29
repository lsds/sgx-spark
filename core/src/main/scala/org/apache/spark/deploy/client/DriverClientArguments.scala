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

package org.apache.spark.deploy.client

import scala.collection.mutable.ListBuffer

/**
 * Command-line parser for the driver client.
 */
private[spark] class DriverClientArguments(args: Array[String]) {
  var cmd: String = "" // 'launch' or 'kill'

  // launch parameters
  var master: String = ""
  var jarUrl: String = ""
  var mainClass: String = ""
  var supervise: Boolean = false
  var memory: Int = 512
  var cores: Int = 1
  private var _driverOptions = ListBuffer[String]()
  def driverOptions = _driverOptions.toSeq

  // kill parameters
  var driverId: String = ""
  
  parse(args.toList)

  def parse(args: List[String]): Unit = args match {
    case ("--cores" | "-c") :: value :: tail =>
      cores = value.toInt
      parse(tail)

    case ("--memory" | "-m") :: value :: tail =>
      memory = value.toInt
      parse(tail)

    case ("--supervise" | "-s") :: tail =>
      supervise = true
      parse(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case "launch" :: _master :: _jarUrl :: _mainClass :: tail =>
      cmd = "launch"
      master = _master
      jarUrl = _jarUrl
      mainClass = _mainClass
      _driverOptions ++= tail

    case "kill" :: _master :: _driverId :: tail =>
      cmd = "kill"
      master = _master
      driverId = _driverId

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    // TODO: It wouldn't be too hard to allow users to submit their app and dependency jars
    //       separately similar to in the YARN client.
    System.err.println(
      "usage: DriverClient [options] launch <active-master> <jar-url> <main-class> " +
        "[driver options]\n" +
      "usage: DriverClient kill <active-master> <driver-id>\n\n" +
      "Options:\n" +
      "  -c CORES, --cores CORES                Number of cores to request \n" +
      "  -m MEMORY, --memory MEMORY             Megabytes of memory to request\n" +
      "  -s, --supervise                        Whether to restart the driver on failure\n")
    System.exit(exitCode)
  }
}
