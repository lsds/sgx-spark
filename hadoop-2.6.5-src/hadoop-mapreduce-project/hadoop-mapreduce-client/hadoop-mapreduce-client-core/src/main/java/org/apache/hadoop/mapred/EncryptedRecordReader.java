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

package org.apache.hadoop.mapred;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.CryptoStreamUtils;
import org.apache.hadoop.crypto.Decryptor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;
import org.apache.spark.sgx.IFillBuffer;
import org.apache.spark.sgx.SgxSettings;
import org.apache.spark.sgx.shm.MallocedMappedDataBuffer;
import org.apache.spark.sgx.shm.MappedDataBuffer;
import org.apache.spark.sgx.shm.MappedDataBufferManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

/**
 * Treats keys as offset in file and value as line. 
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Unstable
public class EncryptedRecordReader implements RecordReader<LongWritable, Text> {
  private static final Log LOG
    = LogFactory.getLog(EncryptedRecordReader.class.getName());

  private long start;
  private long pos;
  private long end;
  private FSDataInputStream fileIn;
  private InputStream in;

  private final boolean sgxEnabled = SgxSettings.SGX_ENABLED();
  private final boolean sgxEnclave = SgxSettings.IS_ENCLAVE();
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int sgxBufferSize;
  private IFillBuffer fillBuffer;
  private MallocedMappedDataBuffer sgxBuffer;
  private int sgxBufferOffset;
  private int sgxBufferAvailableBytes;

  private final byte[] keyBytes = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
          0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17 };
  private final byte[] ivBytes =  new byte[] { 0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x01 };
  private int encryptedBufferSize = DEFAULT_BUFFER_SIZE;
  private byte[] decryptedBuffer;
  private int decryptedBufferOffset;
  private final CipherSuite suite = CipherSuite.AES_CTR_NOPADDING;
  private final CryptoProtocolVersion version = CryptoProtocolVersion.ENCRYPTION_ZONES;
  private FileEncryptionInfo feInfo;
  private CryptoCodec codec;
  private Decryptor decryptor;
  private ByteBuffer inBuffer;
  private ByteBuffer outBuffer;
  private boolean needReadNextRecord;


  /**
   * A class that provides a line reader from an input stream.
   * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
   */
  @Deprecated
  public static class LineReader extends org.apache.hadoop.util.LineReader {
    LineReader(InputStream in) {
      super(in);
    }
    LineReader(InputStream in, int bufferSize) {
      super(in, bufferSize);
    }
    public LineReader(InputStream in, Configuration conf) throws IOException {
      super(in, conf);
    }
    LineReader(InputStream in, byte[] recordDelimiter) {
      super(in, recordDelimiter);
    }
    LineReader(InputStream in, int bufferSize, byte[] recordDelimiter) {
      super(in, bufferSize, recordDelimiter);
    }
    public LineReader(InputStream in, Configuration conf,
        byte[] recordDelimiter) throws IOException {
      super(in, conf, recordDelimiter);
    }
  }

  public EncryptedRecordReader(Configuration job,
                               FileSplit split) throws IOException {
    this(job, split, null);
  }

  public EncryptedRecordReader(Configuration job, FileSplit split,
                               byte[] recordDelimiter) throws IOException {
	  System.out.println("Create new EncryptedRecordReader()");
    fillBuffer = null;
    start = split.getStart();
    end = start + split.getLength();
    pos = start;
    final Path file = split.getPath();

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    fileIn.seek(start);
    in = (InputStream)fileIn;

    if (sgxEnabled) {
      sgxBufferSize = job.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
      sgxBuffer = MappedDataBufferManager.get().malloc(sgxBufferSize);
      System.out.println("EncryptedRecordReader initialize() for non-SGX: sgxBuffer=" + sgxBuffer + ", its size=" + sgxBufferSize);
    }

    sgxBufferOffset = 0;         // used only inside the enclave
    sgxBufferAvailableBytes = 0; // used only inside the enclave

    feInfo = new FileEncryptionInfo(suite, version, keyBytes, ivBytes, "my_great_key", "version_0");
    codec = getCryptoCodec(job, feInfo);
    encryptedBufferSize = CryptoStreamUtils.checkBufferSize(codec, CryptoStreamUtils.getBufferSize(codec.getConf()));

    try {
      decryptor = codec.createDecryptor();
      decryptor.init(keyBytes, ivBytes);
    } catch (GeneralSecurityException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    inBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);
    outBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);

    System.out.println("EncryptedRecordReader constructor for non-SGX: fillBuffer=" + fillBuffer + ", sgxBuffer=" + sgxBuffer + ", its size=" + sgxBufferSize);
    System.out.println("Initialize EncryptedRecordReader (non-SGX) with split " + split.toString() + ", start=" + start + ", end=" + end + ", file=" + file.toString() + ", encryptedBufferSize=" + encryptedBufferSize);

    decryptedBuffer = null;
    decryptedBufferOffset = 0;

    if (!sgxEnabled) {
      getNewEncryptedBuffer();
      if (start != 0) {
        // skip the first record if I'm not the first worker
        // the first record on my split will be read by the worker before me
        next(createKey(), createValue());
      }
      needReadNextRecord = true;  // always try to read a record in the next block
    }
  }

  // SGX
  public EncryptedRecordReader(MallocedMappedDataBuffer buffer, byte[] recordDelimiter, long splitLength, long splitStart, IFillBuffer fillBuffer) throws IOException {
	  System.out.println("Create new EncryptedRecordReader()");
    start = splitStart;
    end = start + splitLength;
    pos = start;
    fileIn = null;
    in = null;

    this.fillBuffer = fillBuffer;
    sgxBuffer = buffer;
    sgxBufferSize = buffer.capacity();


    sgxBufferOffset = 0;         // used only inside the enclave
    sgxBufferAvailableBytes = 0; // used only inside the enclave

    feInfo = new FileEncryptionInfo(suite, version, keyBytes, ivBytes, "my_great_key", "version_0");
    Configuration conf = new Configuration();
    codec = getCryptoCodec(conf, feInfo);
    encryptedBufferSize = CryptoStreamUtils.checkBufferSize(codec, CryptoStreamUtils.getBufferSize(codec.getConf()));

    try {
      decryptor = codec.createDecryptor();
      decryptor.init(keyBytes, ivBytes);
    } catch (GeneralSecurityException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    inBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);
    outBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);

    System.out.println("EncryptedRecordReader constructor for SGX: fillBuffer=" + fillBuffer + ", sgxBuffer=" + sgxBuffer + ", its size=" + sgxBufferSize);
    System.out.println("Initialize EncryptedRecordReader (SGX) with split start=" + start + ", end=" + end + ", encryptedBufferSize=" + encryptedBufferSize);

    decryptedBuffer = null;
    decryptedBufferOffset = 0;

    getNewEncryptedBuffer();
    if (start != 0) {
      // skip the first record if I'm not the first worker
      // the first record on my split will be read by the worker before me
      next(createKey(), createValue());
    }
    needReadNextRecord = true;  // always try to read a record in the next block
  }

  public EncryptedRecordReader(InputStream in, long offset, long endOffset,
                               int maxLineLength) {
    this(in, offset, endOffset, maxLineLength, null);
  }

  public EncryptedRecordReader(InputStream in, long offset, long endOffset,
                               int maxLineLength, byte[] recordDelimiter) {
    this.in = in;
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;
    this.pos = 0;

    try {
      throw new Exception("EncryptedRecordReader(" + in + ", " + offset + ", " + endOffset
                  + ", " + maxLineLength + ", " + recordDelimiter);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public EncryptedRecordReader(InputStream in, long offset, long endOffset,
                               Configuration job)
    throws IOException{
    this(in, offset, endOffset, job, null);
  }

  public EncryptedRecordReader(InputStream in, long offset, long endOffset,
                               Configuration job, byte[] recordDelimiter)
    throws IOException{
    this.in = in;
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;
    this.pos = 0;

    try {
      throw new Exception("EncryptedRecordReader(" + in + ", " + offset + ", " + endOffset
              + ", " + job + ", " + recordDelimiter);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  /**
   * From Hadoop 2.6.5
   * Obtain a CryptoCodec based on the CipherSuite set in a FileEncryptionInfo
   * and the available CryptoCodecs configured in the Configuration.
   *
   * @param conf   Configuration
   * @param feInfo FileEncryptionInfo
   * @return CryptoCodec
   * @throws IOException if no suitable CryptoCodec for the CipherSuite is
   *                     available.
   */
  @SuppressWarnings("Duplicates")
  public static CryptoCodec getCryptoCodec(Configuration conf,
                                            FileEncryptionInfo feInfo) throws IOException {
    final CipherSuite suite = feInfo.getCipherSuite();
    if (suite.equals(CipherSuite.UNKNOWN)) {
      throw new IOException("NameNode specified unknown CipherSuite with ID "
              + suite.getUnknownValue() + ", cannot instantiate CryptoCodec.");
    }
    final CryptoCodec codec = CryptoCodec.getInstance(conf, suite);
    if (codec == null) {
      throw new IOException(
              "No configuration found for the cipher suite "
                      + suite.getConfigSuffix() + " prefixed with "
                      + HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX
                      + ". Please see the example configuration "
                      + "hadoop.security.crypto.codec.classes.EXAMPLECIPHERSUITE "
                      + "at core-default.xml for details.");
    }
    return codec;
  }

  private int readBytesFromFile(byte[] encryptedBuffer) throws IOException {
    int r;

    if (sgxEnabled && sgxEnclave) {
      if (sgxBufferOffset == 0 || sgxBufferOffset >= sgxBufferAvailableBytes) {
        sgxBufferOffset = 0;
        sgxBufferAvailableBytes = fillBuffer.fillBuffer(false); // the boolean is not used for the EncryptedRecordReader
        if (sgxBufferAvailableBytes < 0) {
          System.out.println("End of stream reached (sgx)");
          return sgxBufferAvailableBytes;
        }
      }
      r = (sgxBufferAvailableBytes-sgxBufferOffset < encryptedBuffer.length ?
              sgxBufferAvailableBytes-sgxBufferOffset : encryptedBuffer.length);
      sgxBuffer.get(0, encryptedBuffer, sgxBufferOffset, r);
      sgxBufferOffset += r;
    } else {
      r = in.read(encryptedBuffer);
      if (r < 0) {
        System.out.println("End of stream reached (no sgx)");
        return r;
      }
    }

    return r;
  }

  /* This method is called by the SgxIteratorProvider */
  public int fillBuffer() throws IOException {
    System.out.println("EncryptedRecordReader.fillBuffer()");
    byte[] b = new byte[encryptedBufferSize];
    int r = in.read(b, 0, encryptedBufferSize);
    if (r == -1) {
      // end of stream reached
      System.out.println("fillBuffer: end of stream reached");
      return -1;
    }
    //System.out.println("start=" + start + ", put 0, b=" + b + ", 0, r=" + r
    //    + ", b=[" + (int)b[0] + " " + (int)b[1] + " " + (int)b[2] + "..."
    //    + (int)b[r-3] + " " + (int)b[r-2] + " " + (int)b[r-1] + "]");
    //System.out.println("start=" + start + ", get from " + sgxBuffer + ", sgxBufferOffset=" + sgxBufferOffset + ", r=" + r);
    sgxBuffer.put(0, b, 0, r);
    return r;
  }

  private boolean getNewEncryptedBuffer() throws IOException {
    byte[] encryptedBuffer = new byte[encryptedBufferSize];
    int r = readBytesFromFile(encryptedBuffer);
    if (r < 0) {
      return false;
    }
    inBuffer.put(encryptedBuffer, 0, r);

    inBuffer.flip();
    outBuffer.clear();
    decryptor.decrypt(inBuffer, outBuffer);
    inBuffer.clear();
    outBuffer.flip();
    decryptedBuffer = new byte[r];
    outBuffer.get(decryptedBuffer, 0, r);
    decryptedBufferOffset = 0;

    //if (decryptor.isContextReset()) {
    // note that in practice the context should not be reset. Instead
    // the encryptor/decryptor automatically update their context and the IV
    // is different for each call to encrypt()/decrypt().
    decryptor.init(keyBytes, ivBytes); //PL: keep the same iv everytime
    //}

    return true;
  }

  private int getNextNewLineOffset() {
    int eof;
    for (eof = decryptedBufferOffset; eof < decryptedBuffer.length; eof++) {
      if (decryptedBuffer[eof] == '\n') {
        break;
      }
    }
    return eof;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }
  
  public Text createValue() {
    return new Text();
  }
  
  private boolean isCompressedInput() {
    return false;
  }

  private int maxBytesToConsume(long pos) {
    return Integer.MAX_VALUE;
  }

  public synchronized long getPos() throws IOException {
    return pos;
  }

  public long getBufferOffset() {
    if (!sgxEnabled) {
      throw new RuntimeException("Method only available in SGX mode.");
    }
    return sgxBuffer.offset();
  }

  public int getBufferSize() {
    return sgxBufferSize;
  }

  /** Read a line. */
  public synchronized boolean next(LongWritable key, Text value)
    throws IOException {

    //System.out.println("calling nextkeyValue() with pos=" + pos + ", end=" + end
    //    + ", decryptedBufferOffset=" + decryptedBufferOffset + (decryptedBuffer != null ? ", decryptedBufferLen=" + decryptedBuffer.length : ", null decryptedBuffer"));

    if (pos >= end && !needReadNextRecord) {
      System.out.println("There is nothing anymore. Returning false");
      value.clear();
      return false;
    }

    key.set(pos);
    value.clear();

    // finding next new line character
    boolean needNewBuffer = true;
    while (needNewBuffer) {
      int eof = getNextNewLineOffset();
      // eof >= decryptedBuffer.length => no new line, need a new block
      // eof < decryptedBuffer.length => new line, need to copy to eof-1
      needNewBuffer = (eof >= decryptedBuffer.length);
      eof = (needNewBuffer ? decryptedBuffer.length : eof);
      pos += eof - decryptedBufferOffset + (needNewBuffer ? 0 : 1); // skip the new line character if there is one

      //System.out.println("Gonna append [" + decryptedBufferOffset + ", " + (decryptedBufferOffset+eof-decryptedBufferOffset)
      //    + "[, eof=" + eof + ", decryptedLen=" + decryptedBuffer.length);

      value.append(decryptedBuffer, decryptedBufferOffset, eof-decryptedBufferOffset);
      decryptedBufferOffset = eof + (needNewBuffer ? 0 : 1); // not sure if we need this

      /*
      System.out.println("now value=[[[" + value + "]]], pos=" + pos + ", decryptedBufferOffset="
              + decryptedBufferOffset + ", eof=" + eof + ", char=["
              + (int)decryptedBuffer[(eof == decryptedBuffer.length ? eof-1 : eof)]
              + "], needNewBuffer=" + needNewBuffer);
      */

      if (needNewBuffer) {
        if (!getNewEncryptedBuffer()) {
          // just return the current buffer
          System.out.println("End of stream reached but no new line found");
          value.clear();
          return false;
        }
      }
    }

    // that should be enough to tell the worker to stop reading after the first record
    // in another block
    if (pos > end && needReadNextRecord) {
      needReadNextRecord = false;
    }

    //System.out.println("Return value of size " + value.getLength() + " at pos " + key.toString() + ": [[[" + value + "]]]");
    //System.out.println(key.toString() + " to " + (key.get()+value.getLength()) + ": [[[" + value + "]]]");

    // Next steps:
    //  1) properly parse the records and return them -- done
    //  2) manage multiple encrypted buffers in the same worker -- seems ok
    //  3) manage multiple HDFS blocks (each worker needs to read properly) -- seems ok
    //  4) align the splits -- needed?
    //  5) enable SGX

    return true;
  }

  /**
   * Get the progress within the split
   */
  public synchronized float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public org.apache.hadoop.util.LineReader getLineReader() {
    return null;
  }
}
