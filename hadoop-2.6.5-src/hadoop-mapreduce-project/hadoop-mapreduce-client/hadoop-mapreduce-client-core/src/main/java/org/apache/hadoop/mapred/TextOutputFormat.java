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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.*;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.*;
import org.apache.spark.sgx.SgxSettings;

/**
 * An {@link OutputFormat} that writes plain text files. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextOutputFormat<K, V> extends FileOutputFormat<K, V> {

  protected static class LineRecordWriter<K, V>
          implements RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    private static final boolean USE_HDFS_ENCRYPTION = SgxSettings.USE_HDFS_ENCRYPTION();
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private final byte[] keyBytes = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17 };
    private final byte[] ivBytes =  new byte[] { 0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01 };
    private final CipherSuite suite = CipherSuite.AES_CTR_NOPADDING;
    private final CryptoProtocolVersion version = CryptoProtocolVersion.ENCRYPTION_ZONES;
    private FileEncryptionInfo feInfo;
    private CryptoCodec codec;
    private Decryptor decryptor;
    private byte[] bufferToEncrypt = null;
    private int bufferToEncryptOffset = 0;
    private int encryptedBufferSize = DEFAULT_BUFFER_SIZE;

    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public LineRecordWriter(DataOutputStream out, String keyValueSeparator, Configuration job) {
      System.out.println("Create new LineRecordWriter");
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }

      if (USE_HDFS_ENCRYPTION) {
        bufferToEncrypt = new byte[DEFAULT_BUFFER_SIZE];
        bufferToEncryptOffset = 0;

        try {
          feInfo = new FileEncryptionInfo(suite, version, keyBytes, ivBytes, "my_great_key", "version_0");
          codec = EncryptedRecordReader.getCryptoCodec(job, feInfo);
          encryptedBufferSize = CryptoStreamUtils.checkBufferSize(codec, CryptoStreamUtils.getBufferSize(codec.getConf()));

          decryptor = codec.createDecryptor();
          decryptor.init(keyBytes, ivBytes);
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(-1);
        }
      }
    }

    public LineRecordWriter(DataOutputStream out) { this(out, "\t", null); }

    private void addDataToBufferToEncrypt(byte[] b, int startPos, int len) throws IOException {
      if (USE_HDFS_ENCRYPTION) {
        int remaining = len-startPos;
        int offset = startPos;
        System.out.println("try to copy from " + startPos + ", len = " + len);
        while (remaining > 0) {
          int canCopy = Math.min(len-offset, bufferToEncrypt.length-bufferToEncryptOffset);
          System.out.println("1. remaining = " + remaining + ", offset = " + offset + ", bufferLen = " + bufferToEncrypt.length + ", bufferOffset = " + bufferToEncryptOffset + ", canCopy = " + canCopy);
          System.arraycopy(b, offset, bufferToEncrypt, bufferToEncryptOffset, canCopy);
          remaining -= canCopy;
          offset += canCopy;

          bufferToEncryptOffset += canCopy;
          System.out.println("2. remaining = " + remaining + ", offset = " + offset + ", bufferLen = " + bufferToEncrypt.length + ", bufferOffset = " + bufferToEncryptOffset + ", canCopy = " + canCopy);
          if (bufferToEncryptOffset == bufferToEncrypt.length) {
            System.out.println("need to flush!");
            encryptAndFlushBuffer();
          }
        }
      } else {
        out.write(b, startPos, len);
      }
    }

    private void addDataToBufferToEncrypt(byte[] b) throws IOException {
      addDataToBufferToEncrypt(b, 0, b.length);
    }

    private void encryptAndFlushBuffer() throws IOException {
      if (USE_HDFS_ENCRYPTION && bufferToEncryptOffset > 0) {
        ByteBuffer inBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);
        ByteBuffer outBuffer = ByteBuffer.allocateDirect(encryptedBufferSize);
        byte[] encryptedBuffer = new byte[bufferToEncryptOffset];

        inBuffer.put(bufferToEncrypt, 0, bufferToEncryptOffset);
        inBuffer.flip();
        outBuffer.clear();
        decryptor.decrypt(inBuffer, outBuffer);
        inBuffer.clear();
        outBuffer.flip();
        outBuffer.get(encryptedBuffer, 0, bufferToEncryptOffset);

        out.write(encryptedBuffer, 0, bufferToEncryptOffset);
        bufferToEncryptOffset = 0;
        decryptor.init(keyBytes, ivBytes); //PL: keep the same iv everytime
      }
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        addDataToBufferToEncrypt(to.getBytes(), 0, to.getLength());
      } else {
        addDataToBufferToEncrypt(o.toString().getBytes(utf8));
      }
    }

    public synchronized void write(K key, V value)
            throws IOException {
      System.out.println("LineRecordWriter.write(" + key + ", " + value + ")");

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        System.out.println("write keyValueSeparator");
        addDataToBufferToEncrypt(keyValueSeparator);
      }
      if (!nullValue) {
        System.out.println("write value");
        writeObject(value);
      }
      System.out.println("write new line");
      addDataToBufferToEncrypt(newline);
    }

    public synchronized void close(Reporter reporter) throws IOException {
      System.out.println("Close LineRecordWriter");
      encryptAndFlushBuffer();
      out.close();
    }
  }

  public RecordWriter<K, V> getRecordWriter(FileSystem ignored,
                                            JobConf job,
                                            String name,
                                            Progressable progress)
          throws IOException {
    boolean isCompressed = getCompressOutput(job);
    String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator",
            "\t");
    if (!isCompressed) {
      Path file = FileOutputFormat.getTaskOutputPath(job, name);
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      System.out.println("Create new LineRecordWriter " + file.toString());
      return new LineRecordWriter<K, V>(fileOut, keyValueSeparator, job);
    } else {
      Class<? extends CompressionCodec> codecClass =
              getOutputCompressorClass(job, GzipCodec.class);
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
      // build the filename including the extension
      Path file =
              FileOutputFormat.getTaskOutputPath(job,
                      name + codec.getDefaultExtension());
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LineRecordWriter<K, V>(new DataOutputStream
              (codec.createOutputStream(fileOut)),
              keyValueSeparator, job);
    }
  }
}

