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

package org.apache.spark.sgx;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.spark.sgx.shm.MappedDataBuffer;
import org.apache.spark.sgx.shm.RingBuffConsumer;
import org.apache.spark.sgx.shm.RingBuffLibWrapper;
import org.apache.spark.sgx.shm.RingBuffProducer;

public class ShmRingBenchmark {

  // 2147483647 = 2.14GB
  private static int size = 2147483647;
  private static long[] handles = RingBuffLibWrapper.init_shm("sgx-lkl-shmem-driver", size);
  private static volatile long count = 0l;

  private static class TestConsumerThread implements Runnable {

    private RingBuffConsumer reader;

    public TestConsumerThread() {
      reader = new RingBuffConsumer(new MappedDataBuffer(handles[0], size), Serialization.serializer);
    }

    @Override
    public void run() {
      while (true) {
        reader.readAny();
        count++;
      }

    }
  }

  private static class TestProducerThread implements Runnable {

    private RingBuffProducer writer;
    private byte[] bytes = new byte[1024];

    public TestProducerThread(){
      writer = new RingBuffProducer(new MappedDataBuffer(handles[0], size), Serialization.serializer);
      Arrays.fill( bytes, (byte) 1 );
    }


    @Override
    public void run() {
      Random rand = new Random();
      while (true) {
        writer.writeAny(bytes);
      }
    }
  }

  public static void main(String[] args) {

    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.execute(new TestProducerThread());
    executor.execute(new TestConsumerThread());

    long start = System.currentTimeMillis();
    int runtimeSecs = 30;

    while ((System.currentTimeMillis() - start)/1000 < runtimeSecs) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    System.out.println("Done");

    executor.shutdown();

    System.out.println("SharedMemory: " + count + " bytes-chunks (1KB) in " + runtimeSecs + " seconds" );
    System.out.println("Throughput: "+ ((count * 1024l)/runtimeSecs)/1024l + " KB/sec");
  }
  /**
   * MemoryShared: 4558173 Ints (4bytes) in 30 seconds
   * Throughput: 593 KB/sec
   *
   * SharedMemory: 4558170 byte-chunks (100bytes) in 30 seconds
   * Throughput: 1.5 MB/sec
   *
   * SharedMemory: 506463 bytes-chunks (1KB) in 30 seconds
   * Throughput: 16 MB/sec
   *
   * // TODO: Use static Object Pool, test plain Int, Long and ShMessages
   *
   */

}
