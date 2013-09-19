package org.apache.spark.deploy.master

import org.apache.spark.Logging
import org.apache.zookeeper._

import akka.serialization.Serialization

class ZooKeeperPersistenceEngine(serialization: Serialization) extends PersistenceEngine with SparkZooKeeperWatcher with Logging {
  val WORKING_DIR = System.getProperty("spark.deploy.zookeeper.dir", "/spark") + "/master_status"

  val zk = new SparkZooKeeperSession(this)

  zk.connect()

  override def zkSessionCreated() {
    zk.mkdirRecursive(WORKING_DIR)
  }

  override def zkDown() {
    logError("PersistenceEngine disconnected from ZooKeeper -- ZK looks down.")
  }

  override def addApplication(app: ApplicationInfo) {
    serializeIntoFile(WORKING_DIR + "/app_" + app.id, app)
  }

  override def removeApplication(app: ApplicationInfo) {
    zk.delete(WORKING_DIR + "/app_" + app.id)
  }

  override def addWorker(worker: WorkerInfo) {
    serializeIntoFile(WORKING_DIR + "/worker_" + worker.id, worker)
  }

  override def removeWorker(worker: WorkerInfo) {
    zk.delete(WORKING_DIR + "/worker_" + worker.id)
  }

  override def close() {
    zk.close()
  }

  override def readPersistedData(): (Seq[ApplicationInfo], Seq[WorkerInfo]) = {
    val sortedFiles = zk.getChildren(WORKING_DIR).toList.sorted
    val appFiles = sortedFiles.filter(_.startsWith("app_"))
    val apps = appFiles.map(deserializeFromFile[ApplicationInfo])
    val workerFiles = sortedFiles.filter(_.startsWith("worker_"))
    val workers = workerFiles.map(deserializeFromFile[WorkerInfo])
    (apps, workers)
  }

  private def serializeIntoFile(path: String, value: Serializable) {
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    zk.create(path, serialized, CreateMode.PERSISTENT)
  }

  def deserializeFromFile[T <: Serializable](filename: String)(implicit m: Manifest[T]): T = {
    val fileData = zk.getData("/spark/master_status/" + filename)
    val clazz = m.erasure.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    serializer.fromBinary(fileData).asInstanceOf[T]
  }
}
