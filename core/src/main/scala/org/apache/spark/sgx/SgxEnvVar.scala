package org.apache.spark.sgx

import org.apache.spark.internal.Logging
import com.google.common.net.InetAddresses

case class SgxEnvVar() extends Logging {
    
}

object SgxEnvVar extends Logging {
  def getIpFromEnvVarOrAbort(envvar: String): String = {
    val addr = sys.env.get(envvar).getOrElse("")
    if (!InetAddresses.isInetAddress(addr)) {
      logError("Must define InetAddress within environment variable " + envvar + ". Aborting.")
    }
    addr
  }

  def getPortFromEnvVarOrAbort(envvar: String): Int = {
    val port = sys.env.get(envvar).getOrElse("0").toInt
    if (port <= 0 || port > 65536) {
      logError("Must define port number within environment variable " + envvar + ". Aborting.")
    }
    port
  }
}