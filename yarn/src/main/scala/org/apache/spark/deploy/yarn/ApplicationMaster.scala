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

package org.apache.spark.deploy.yarn

import java.io.IOException
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.util.Utils

class ApplicationMaster(args: ApplicationMasterArguments, conf: Configuration, sparkConf: SparkConf) extends Logging {

  def this(args: ApplicationMasterArguments,sparkConf: SparkConf) = this(args, new Configuration(), sparkConf)

  def this(args: ApplicationMasterArguments) = this(args, new SparkConf())

  private val rpc: YarnRPC = YarnRPC.create(conf)
  private var resourceManager: AMRMProtocol = _
  private var appAttemptId: ApplicationAttemptId = _
  private var userThread: Thread = _
  private val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  private val fs = FileSystem.get(yarnConf)

  private var yarnAllocator: YarnAllocationHandler = _
  private var isFinished: Boolean = false
  private var uiAddress: String = _
  private val maxAppAttempts: Int = conf.getInt(YarnConfiguration.RM_AM_MAX_RETRIES,
    YarnConfiguration.DEFAULT_RM_AM_MAX_RETRIES)
  private var isLastAMRetry: Boolean = true

  // Default to numWorkers * 2, with minimum of 3
  private val maxNumWorkerFailures = sparkConf.getInt("spark.yarn.max.worker.failures",
    math.max(args.numWorkers * 2, 3))

  def run() {
    // Setup the directories so things go to yarn approved directories rather
    // then user specified and /tmp.
    System.setProperty("spark.local.dir", getLocalDirs())

    // set the web ui port to be ephemeral for yarn so we don't conflict with
    // other spark processes running on the same box
    System.setProperty("spark.ui.port", "0")

    // Use priority 30 as its higher then HDFS. Its same priority as MapReduce is using.
    ShutdownHookManager.get().addShutdownHook(new AppMasterShutdownHook(this), 30)

    appAttemptId = getApplicationAttemptId()
    isLastAMRetry = appAttemptId.getAttemptId() >= maxAppAttempts
    resourceManager = registerWithResourceManager()

    // Workaround until hadoop moves to something which has
    // https://issues.apache.org/jira/browse/HADOOP-8406 - fixed in (2.0.2-alpha but no 0.23 line)
    // ignore result.
    // This does not, unfortunately, always work reliably ... but alleviates the bug a lot of times
    // Hence args.workerCores = numCore disabled above. Any better option?

    // Compute number of threads for akka
    //val minimumMemory = appMasterResponse.getMinimumResourceCapability().getMemory()
    //if (minimumMemory > 0) {
    //  val mem = args.workerMemory + YarnAllocationHandler.MEMORY_OVERHEAD
    //  val numCore = (mem  / minimumMemory) + (if (0 != (mem % minimumMemory)) 1 else 0)

    //  if (numCore > 0) {
        // do not override - hits https://issues.apache.org/jira/browse/HADOOP-8406
        // TODO: Uncomment when hadoop is on a version which has this fixed.
        // args.workerCores = numCore
    //  }
    //}
    // org.apache.hadoop.io.compress.CompressionCodecFactory.getCodecClasses(conf)

    ApplicationMaster.register(this)
    // Start the user's JAR
    userThread = startUserClass()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    waitForSparkContextInitialized()

    // Do this after spark master is up and SparkContext is created so that we can register UI Url
    val appMasterResponse: RegisterApplicationMasterResponse = registerApplicationMaster()

    // Allocate all containers
    allocateWorkers()

    // Wait for the user class to Finish
    userThread.join()

    System.exit(0)
  }

  /** Get the Yarn approved local directories. */
  private def getLocalDirs(): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(System.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(System.getenv("LOCAL_DIRS"))
        .getOrElse(""))

    if (localDirs.isEmpty()) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  private def getApplicationAttemptId(): ApplicationAttemptId = {
    val envs = System.getenv()
    val containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV)
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    logInfo("ApplicationAttemptId: " + appAttemptId)
    appAttemptId
  }

  private def registerWithResourceManager(): AMRMProtocol = {
    val rmAddress = NetUtils.createSocketAddr(yarnConf.get(
      YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    logInfo("Connecting to ResourceManager at " + rmAddress)
    rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]
  }

  private def registerApplicationMaster(): RegisterApplicationMasterResponse = {
    logInfo("Registering the ApplicationMaster")
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
      .asInstanceOf[RegisterApplicationMasterRequest]
    appMasterRequest.setApplicationAttemptId(appAttemptId)
    // Setting this to master host,port - so that the ApplicationReport at client has some
    // sensible info.
    // Users can then monitor stderr/stdout on that node if required.
    appMasterRequest.setHost(Utils.localHostName())
    appMasterRequest.setRpcPort(0)
    appMasterRequest.setTrackingUrl(uiAddress)
    resourceManager.registerApplicationMaster(appMasterRequest)
  }

  private def startUserClass(): Thread  = {
    logInfo("Starting the user JAR in a separate Thread")
    val mainMethod = Class.forName(
      args.userClass,
      false /* initialize */,
      Thread.currentThread.getContextClassLoader).getMethod("main", classOf[Array[String]])
    val t = new Thread {
      override def run() {
        var successed = false
        try {
          // Copy
          var mainArgs: Array[String] = new Array[String](args.userArgs.size)
          args.userArgs.copyToArray(mainArgs, 0, args.userArgs.size)
          mainMethod.invoke(null, mainArgs)
          // some job script has "System.exit(0)" at the end, for example SparkPi, SparkLR
          // userThread will stop here unless it has uncaught exception thrown out
          // It need shutdown hook to set SUCCEEDED
          successed = true
        } finally {
          logDebug("finishing main")
          isLastAMRetry = true
          if (successed) {
            ApplicationMaster.this.finishApplicationMaster(FinalApplicationStatus.SUCCEEDED)
          } else {
            ApplicationMaster.this.finishApplicationMaster(FinalApplicationStatus.FAILED)
          }
        }
      }
    }
    t.start()
    t
  }

  // this need to happen before allocateWorkers
  private def waitForSparkContextInitialized() {
    logInfo("Waiting for spark context initialization")
    try {
      var sparkContext: SparkContext = null
      ApplicationMaster.sparkContextRef.synchronized {
        var count = 0
        val waitTime = 10000L
        val numTries = sparkConf.getInt("spark.yarn.ApplicationMaster.waitTries", 10)
        while (ApplicationMaster.sparkContextRef.get() == null && count < numTries) {
          logInfo("Waiting for spark context initialization ... " + count)
          count = count + 1
          ApplicationMaster.sparkContextRef.wait(waitTime)
        }
        sparkContext = ApplicationMaster.sparkContextRef.get()
        assert(sparkContext != null || count >= numTries)

        if (null != sparkContext) {
          uiAddress = sparkContext.ui.appUIAddress
          this.yarnAllocator = YarnAllocationHandler.newAllocator(
            yarnConf,
            resourceManager,
            appAttemptId,
            args,
            sparkContext.preferredNodeLocationData,
            sparkContext.getConf)
        } else {
          logWarning("Unable to retrieve sparkContext inspite of waiting for %d, numTries = %d".
            format(count * waitTime, numTries))
          this.yarnAllocator = YarnAllocationHandler.newAllocator(
            yarnConf,
            resourceManager,
            appAttemptId,
            args, 
            sparkContext.getConf)
        }
      }
    } finally {
      // in case of exceptions, etc - ensure that count is atleast ALLOCATOR_LOOP_WAIT_COUNT :
      // so that the loop (in ApplicationMaster.sparkContextInitialized) breaks
      ApplicationMaster.incrementAllocatorLoop(ApplicationMaster.ALLOCATOR_LOOP_WAIT_COUNT)
    }
  }

  private def allocateWorkers() {
    try {
      logInfo("Allocating " + args.numWorkers + " workers.")
      // Wait until all containers have finished
      // TODO: This is a bit ugly. Can we make it nicer?
      // TODO: Handle container failure

      // Exists the loop if the user thread exits.
      while (yarnAllocator.getNumWorkersRunning < args.numWorkers && userThread.isAlive) {
        if (yarnAllocator.getNumWorkersFailed >= maxNumWorkerFailures) {
          finishApplicationMaster(FinalApplicationStatus.FAILED,
            "max number of worker failures reached")
        }
        yarnAllocator.allocateContainers(
          math.max(args.numWorkers - yarnAllocator.getNumWorkersRunning, 0))
        ApplicationMaster.incrementAllocatorLoop(1)
        Thread.sleep(100)
      }
    } finally {
      // In case of exceptions, etc - ensure that count is at least ALLOCATOR_LOOP_WAIT_COUNT,
      // so that the loop in ApplicationMaster#sparkContextInitialized() breaks.
      ApplicationMaster.incrementAllocatorLoop(ApplicationMaster.ALLOCATOR_LOOP_WAIT_COUNT)
    }
    logInfo("All workers have launched.")

    // Launch a progress reporter thread, else the app will get killed after expiration
    // (def: 10mins) timeout.
    // TODO(harvey): Verify the timeout
    if (userThread.isAlive) {
      // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
      val timeoutInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

      // we want to be reasonably responsive without causing too many requests to RM.
      val schedulerInterval =
        sparkConf.getLong("spark.yarn.scheduler.heartbeat.interval-ms", 5000)

      // must be <= timeoutInterval / 2.
      val interval = math.min(timeoutInterval / 2, schedulerInterval)

      launchReporterThread(interval)
    }
  }

  private def launchReporterThread(_sleepTime: Long): Thread = {
    val sleepTime = if (_sleepTime <= 0 ) 0 else _sleepTime

    val t = new Thread {
      override def run() {
        while (userThread.isAlive) {
          if (yarnAllocator.getNumWorkersFailed >= maxNumWorkerFailures) {
            finishApplicationMaster(FinalApplicationStatus.FAILED,
              "max number of worker failures reached")
          }
          val missingWorkerCount = args.numWorkers - yarnAllocator.getNumWorkersRunning
          if (missingWorkerCount > 0) {
            logInfo("Allocating %d containers to make up for (potentially) lost containers".
              format(missingWorkerCount))
            yarnAllocator.allocateContainers(missingWorkerCount)
          }
          else sendProgress()
          Thread.sleep(sleepTime)
        }
      }
    }
    // Setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.start()
    logInfo("Started progress reporter thread - sleep time : " + sleepTime)
    t
  }

  private def sendProgress() {
    logDebug("Sending progress")
    // Simulated with an allocate request with no nodes requested ...
    yarnAllocator.allocateContainers(0)
  }

  /*
  def printContainers(containers: List[Container]) = {
    for (container <- containers) {
      logInfo("Launching shell command on a new container."
        + ", containerId=" + container.getId()
        + ", containerNode=" + container.getNodeId().getHost()
        + ":" + container.getNodeId().getPort()
        + ", containerNodeURI=" + container.getNodeHttpAddress()
        + ", containerState" + container.getState()
        + ", containerResourceMemory"
        + container.getResource().getMemory())
    }
  }
  */

  def finishApplicationMaster(status: FinalApplicationStatus, diagnostics: String = "") {
    synchronized {
      if (isFinished) {
        return
      }
      isFinished = true
    }

    logInfo("finishApplicationMaster with " + status)
    val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest])
      .asInstanceOf[FinishApplicationMasterRequest]
    finishReq.setAppAttemptId(appAttemptId)
    finishReq.setFinishApplicationStatus(status)
    finishReq.setDiagnostics(diagnostics)
    // Set tracking url to empty since we don't have a history server.
    finishReq.setTrackingUrl("")
    resourceManager.finishApplicationMaster(finishReq)
  }

  /**
   * Clean up the staging directory.
   */
  private def cleanupStagingDir() {
    var stagingDirPath: Path = null
    try {
      val preserveFiles = sparkConf.get("spark.yarn.preserve.staging.files", "false").toBoolean
      if (!preserveFiles) {
        stagingDirPath = new Path(System.getenv("SPARK_YARN_STAGING_DIR"))
        if (stagingDirPath == null) {
          logError("Staging directory is null")
          return
        }
        logInfo("Deleting staging directory " + stagingDirPath)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logError("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  // The shutdown hook that runs when a signal is received AND during normal close of the JVM.
  class AppMasterShutdownHook(appMaster: ApplicationMaster) extends Runnable {

    def run() {
      logInfo("AppMaster received a signal.")
      // we need to clean up staging dir before HDFS is shut down
      // make sure we don't delete it until this is the last AM
      if (appMaster.isLastAMRetry) appMaster.cleanupStagingDir()
    }
  }
}

object ApplicationMaster {
  // Number of times to wait for the allocator loop to complete.
  // Each loop iteration waits for 100ms, so maximum of 3 seconds.
  // This is to ensure that we have reasonable number of containers before we start
  // TODO: Currently, task to container is computed once (TaskSetManager) - which need not be
  // optimal as more containers are available. Might need to handle this better.
  private val ALLOCATOR_LOOP_WAIT_COUNT = 30
  def incrementAllocatorLoop(by: Int) {
    val count = yarnAllocatorLoop.getAndAdd(by)
    if (count >= ALLOCATOR_LOOP_WAIT_COUNT) {
      yarnAllocatorLoop.synchronized {
        // to wake threads off wait ...
        yarnAllocatorLoop.notifyAll()
      }
    }
  }

  private val applicationMasters = new CopyOnWriteArrayList[ApplicationMaster]()

  def register(master: ApplicationMaster) {
    applicationMasters.add(master)
  }

  val sparkContextRef: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null /* initialValue */)
  val yarnAllocatorLoop: AtomicInteger = new AtomicInteger(0)

  def sparkContextInitialized(sc: SparkContext): Boolean = {
    var modified = false
    sparkContextRef.synchronized {
      modified = sparkContextRef.compareAndSet(null, sc)
      sparkContextRef.notifyAll()
    }

    // Add a shutdown hook - as a best case effort in case users do not call sc.stop or do
    // System.exit.
    // Should not really have to do this, but it helps YARN to evict resources earlier.
    // Not to mention, prevent the Client from declaring failure even though we exited properly.
    // Note that this will unfortunately not properly clean up the staging files because it gets
    // called too late, after the filesystem is already shutdown.
    if (modified) {
      Runtime.getRuntime().addShutdownHook(new Thread with Logging {
        // This is not only logs, but also ensures that log system is initialized for this instance
        // when we are actually 'run'-ing.
        logInfo("Adding shutdown hook for context " + sc)
        override def run() {
          logInfo("Invoking sc stop from shutdown hook")
          sc.stop()
          // Best case ...
          for (master <- applicationMasters) {
            master.finishApplicationMaster(FinalApplicationStatus.SUCCEEDED)
          }
        }
      } )
    }

    // Wait for initialization to complete and atleast 'some' nodes can get allocated.
    yarnAllocatorLoop.synchronized {
      while (yarnAllocatorLoop.get() <= ALLOCATOR_LOOP_WAIT_COUNT) {
        yarnAllocatorLoop.wait(1000L)
      }
    }
    modified
  }

  def main(argStrings: Array[String]) {
    val args = new ApplicationMasterArguments(argStrings)
    new ApplicationMaster(args).run()
  }
}
