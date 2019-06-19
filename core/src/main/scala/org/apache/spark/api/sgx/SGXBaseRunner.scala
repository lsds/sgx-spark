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

import java.io._
import java.net.{ServerSocket, Socket}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.reflect.{ClassTag}

private[spark] abstract class SGXBaseRunner[IN: ClassTag, OUT: ClassTag](
                                           func: (Iterator[Any]) => Any,
                                           evalType: Int) extends Logging {

  private val conf = SparkEnv.get.conf
  private val bufferSize = conf.getInt("spark.buffer.size", 65536)

  val closureSer = SparkEnv.get.closureSerializer.newInstance()
  val iteratorSer = SparkEnv.get.serializer.newInstance()

  protected val envVars = collection.mutable.Map[String, String]()
  // Expose a ServerSocket to support method calls via socket from SGX side
  private[spark] var serverSocket: Option[ServerSocket] = None


  def compute(inputIterator: Iterator[IN],
              partitionIndex: Int,
              context: TaskContext): Iterator[OUT] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread

    val worker: Socket = env.createSGXWorker(envVars.toMap)
    // Whether is the worker released into idle pool or closed
    // TODO: Reuse workers from the pool
    val releasedOrClosed = new AtomicBoolean(false)

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = sgxWriterThread(env, worker, inputIterator, partitionIndex, context)
    // Add task completion Listener
    context.addTaskCompletionListener[Unit] { _ =>
      writerThread.shutdownOnTaskCompletion()
      if (releasedOrClosed.compareAndSet(false, true)) {
        try {
          worker.close()
        } catch {
          case e: Exception =>
            logWarning("Failed to close worker socket", e)
        }
      }
    }

    writerThread.start()
    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    val stdoutIterator = new ReaderIterator[OUT](stream, writerThread, startTime, env, worker, releasedOrClosed, context)
    logInfo(s"SGX compute iterator read done")
    new InterruptibleIterator(context, stdoutIterator)
  }

  // TODO: Implement SharedMemory / Arrow support
  protected def sgxWriterThread(env: SparkEnv,
                                worker: Socket,
                                inputIterator: Iterator[IN],
                                partitionIndex: Int,
                                context: TaskContext): WriterIterator

  /**
    * The thread responsible for writing the data from the SGXRDD's parent iterator to the
    * SGX secure Worker
    */
  abstract class WriterIterator(env: SparkEnv,
                                      worker: Socket,
                                      inputIterator: Iterator[IN],
                                      partitionIndex: Int,
                                      context: TaskContext)
    extends Thread(s"stdout writer for SGXRunner TID:${context.taskAttemptId()}") {

    @volatile private var _exception: Exception = null

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the SGX worker */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.isCompleted)
      this.interrupt()
    }

    /** Writes a command section to the stream connected to the SGX worker */
    protected def writeFunction(dataOut: DataOutputStream): Unit

    /** Writes input data to the stream connected to the SGX worker */
    protected def writeIteratorToStream(dataOut: DataOutputStream): Unit

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        TaskContext.setTaskContext(context)
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // SGX version of driver
        val sgxVer = "999"
        SGXRDD.writeUTF(sgxVer, dataOut)
        // TODO: Add barrier support
        // Close ServerSocket on task completion.
        serverSocket.foreach { server =>
          context.addTaskCompletionListener[Unit](_ => server.close())
        }
        // Init a ServerSocket to accept method calls from SGX side.
        val boundPort: Int = serverSocket.map(_.getLocalPort).getOrElse(0)
        if (boundPort == -1) {
          val message = "ServerSocket failed to bind to Java side."
          logError(message)
          throw new SparkException(message)
        }
        // Write out the TaskContextInfo
        dataOut.writeInt(boundPort)
        dataOut.writeInt(context.stageId())
        dataOut.writeInt(context.partitionId())
        dataOut.writeInt(context.attemptNumber())
        dataOut.writeLong(context.taskAttemptId())
        val localProps = context.getLocalProperties.asScala
        dataOut.writeInt(localProps.size)
        localProps.foreach { case (k, v) =>
          SGXRDD.writeUTF(k, dataOut)
          SGXRDD.writeUTF(v, dataOut)
        }

        // sparkFilesDir
        SGXRDD.writeUTF(SparkFiles.getRootDirectory(), dataOut)
        // TODO PANOS: ignore encrypted Broadcast variables for now
        dataOut.flush()

        // Write Function Type & Function
        dataOut.writeInt(evalType)
        writeFunction(dataOut)

        // Write OUT Iterator
        writeIteratorToStream(dataOut)
        dataOut.writeInt(SpecialSGXChars.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }
      }
    }
  }

  // OUT => Array[Byte] for encrypted data
  class ReaderIterator[OUT: ClassTag](stream: DataInputStream,
                                                    writerThread: WriterIterator,
                                                    startTime: Long,
                                                    env: SparkEnv,
                                                    worker: Socket,
                                                    releasedOrClosed: AtomicBoolean,
                                                    context: TaskContext)
    extends Iterator[OUT] {

    private var nextObj: OUT = _
    private var eos = false

    override def hasNext: Boolean = nextObj != null || {
      if (!eos) {
        nextObj = read()
        hasNext
      } else {
        false
      }
    }

    override def next(): OUT = {
      if (hasNext) {
        val obj = nextObj
        nextObj = null.asInstanceOf[OUT]
        logDebug(s"SGX => Reading ${obj}")
        obj
      } else {
        Iterator.empty.next()
      }
    }

    /**
      * Reads next object from the stream.
      * When the stream reaches end of data, needs to process the following sections,
      * and then returns null.
      */
    protected def read(): OUT = {
      if (writerThread.exception.isDefined) {
        throw writerThread.exception.get
      }
      try {
        stream.readInt() match {
          case length if length > 0 =>
            val obj = new Array[Byte](length)
            stream.readFully(obj)
            // Not necessary of we are dealing just with bytes
            return iteratorSer.deserialize[OUT](ByteBuffer.wrap(obj))
          case SpecialSGXChars.EMPTY_DATA =>
            // Array.empty[Byte]
            null.asInstanceOf[OUT]
          case SpecialSGXChars.TIMING_DATA =>
            handleTimingData()
            read()
          case SpecialSGXChars.SGX_EXCEPTION_THROWN =>
            throw handleSGXException()
          case SpecialSGXChars.END_OF_DATA_SECTION =>
            // TODO PANOs: Send stats
            handleEndOfDataSection()
            null.asInstanceOf[OUT]
        }
      } catch handleException
    }

    protected def handleTimingData(): Unit = {
      // Timing data from worker
      val bootTime = stream.readLong()
      val initTime = stream.readLong()
      val finishTime = stream.readLong()
      val boot = bootTime - startTime
      val init = initTime - bootTime
      val finish = finishTime - initTime
      val total = finishTime - startTime
      logInfo("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
        init, finish))
      val memoryBytesSpilled = stream.readLong()
      val diskBytesSpilled = stream.readLong()
      context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
      context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
    }

    protected def handleSGXException(): SGXException = {
      // Signals that an exception has been thrown in python
      val exLength = stream.readInt()
      val obj = new Array[Byte](exLength)
      stream.readFully(obj)
      new SGXException(new String(obj, StandardCharsets.UTF_8),
        writerThread.exception.getOrElse(null))
    }

    protected def handleEndOfDataSection(): Unit = {
      // We've finished the data section of the output, but we can still
      // read some accumulator updates:
      //      val numAccumulatorUpdates = stream.readInt()
      //      (1 to numAccumulatorUpdates).foreach { _ =>
      //        val updateLen = stream.readInt()
      //        val update = new Array[Byte](updateLen)
      //        stream.readFully(update)
      //        //        accumulator.add(update)
      //      }
      // Check whether the worker is ready to be re-used.
      if (stream.readInt() == SpecialSGXChars.END_OF_STREAM) {
        if (releasedOrClosed.compareAndSet(false, true)) {
          logWarning("SGX Worker now ready to be released!")
          // env.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
        }
      }
      eos = true
    }

    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception if context.isInterrupted =>
        logDebug("Exception thrown after task interruption", e)
        throw new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))

      case e: Exception if writerThread.exception.isDefined =>
        logError("SGX worker exited unexpectedly (crashed)", e)
        logError("This may have been caused by a prior exception:", writerThread.exception.get)
        throw writerThread.exception.get

      case eof: EOFException =>
        throw new SparkException("SGX worker exited unexpectedly (crashed)", eof)

      null.asInstanceOf[OUT]
    }
  }
}

private[spark] object SGXFunctionType {
  val NON_UDF = 0
  val BATCHED_UDF = 100
  def toString(sgxFuncType: Int): String = sgxFuncType match {
    case NON_UDF => "NON_UDF"
    case BATCHED_UDF => "BATCHED_UDF"
  }
}

private[spark] object SpecialSGXChars {
  val EMPTY_DATA = 0
  val END_OF_DATA_SECTION = -1
  val SGX_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
  val START_ARROW_STREAM = -6
}
