/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.spark.sgx.IFillBuffer;
import org.apache.spark.sgx.SgxSettings;
import org.apache.spark.sgx.shm.MallocedMappedDataBuffer;
import org.apache.spark.sgx.shm.MappedDataBuffer;

/**
 * SplitLineReader for uncompressed files.
 * This class can split the file correctly even if the delimiter is multi-bytes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class UncompressedSplitLineReader extends SplitLineReader {
  private boolean needAdditionalRecord = false;
  private long splitLength;
  /** Total bytes read from the input stream. */
  private long totalBytesRead = 0;
  private boolean finished = false;
  private boolean usingCRLF;
  private IFillBuffer fillBuffer = null;
  private final boolean sgxEnabled = SgxSettings.SGX_ENABLED();

  public UncompressedSplitLineReader(FSDataInputStream in, Configuration conf,
      byte[] recordDelimiterBytes, long splitLength) throws IOException {
    super(in, conf, recordDelimiterBytes);
    this.splitLength = splitLength;
    usingCRLF = (recordDelimiterBytes == null);
  }
  
  public UncompressedSplitLineReader(MallocedMappedDataBuffer buffer, byte[] recordDelimiterBytes, long splitLength, IFillBuffer fillBuffer) throws IOException {
	    super(buffer, recordDelimiterBytes);
	    this.splitLength = splitLength;
	    this.fillBuffer = fillBuffer;
	    usingCRLF = (recordDelimiterBytes == null);
	  }  
  
  private int read(InputStream in, MappedDataBuffer buffer, int off, int len) throws IOException {	  
      if (buffer == null) {
          throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > buffer.capacity() - off) {
          throw new IndexOutOfBoundsException();
      } else if (len == 0) {
          return 0;
      }

      int c = in.read();
      if (c == -1) {
          return -1;
      }
      buffer.put(off, (byte)c);

      int i = 1;
      try {
          for (; i < len ; i++) {
              c = in.read();
              if (c == -1) {
                  break;
              }
              buffer.put(off + i, (byte)c);
          }
      } catch (IOException ee) {
      }
      return i;
  }  

  @Override
  protected int fillBuffer(InputStream in, MappedDataBuffer buffer, boolean inDelimiter)
      throws IOException {
	    
	if (sgxEnabled && SgxSettings.IS_ENCLAVE()) {
		return fillBuffer.fillBuffer(inDelimiter);
	}
	  
    int maxBytesToRead = buffer.capacity();
    if (totalBytesRead < splitLength) {
      long leftBytesForSplit = splitLength - totalBytesRead;
      // check if leftBytesForSplit exceed Integer.MAX_VALUE
      if (leftBytesForSplit <= Integer.MAX_VALUE) {
        maxBytesToRead = Math.min(maxBytesToRead, (int)leftBytesForSplit);
      }
    }
    
    int bytesRead = read(in, buffer, 0, maxBytesToRead);    

    // If the split ended in the middle of a record delimiter then we need
    // to read one additional record, as the consumer of the next split will
    // not recognize the partial delimiter as a record.
    // However if using the default delimiter and the next character is a
    // linefeed then next split will treat it as a delimiter all by itself
    // and the additional record read should not be performed.
    if (totalBytesRead == splitLength && inDelimiter && bytesRead > 0) {
      if (usingCRLF) {
        needAdditionalRecord = (buffer.get(0) != '\n');
      } else {
        needAdditionalRecord = true;
      }
    }
    if (bytesRead > 0) {
      totalBytesRead += bytesRead;
    }

    return bytesRead;
  }

  @Override
  public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
	  
    int bytesRead = 0;
    if (!finished) {
      // only allow at most one more record to be read after the stream
      // reports the split ended
      if (totalBytesRead > splitLength) {
        finished = true;
      }

      bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
    }
    return bytesRead;
  }

  @Override
  public boolean needAdditionalRecordAfterSplit() {
    return !finished && needAdditionalRecord;
  }

  @Override
  protected void unsetNeedAdditionalRecordAfterSplit() {
    needAdditionalRecord = false;
  }  
}
