package org.apache.spark.sgx.shm;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.sgx.Serialization;
import org.apache.spark.sgx.SgxSettings;

/**
 * This class is to be used by the enclave to communicate with the outside.
 * 
 * @author Florian Kelbert
 *
 */
public final class ShmCommunicationManager<T> implements Callable<T> {
	private static boolean initialized = false;
	private static RingBuffConsumer reader;
	private static RingBuffProducer writer;

	private static Map<Long, BlockingQueue<Object>> inboxes = new ConcurrentHashMap<>();
	private static BlockingQueue<ShmCommunicator> accepted = new LinkedBlockingQueue<>();
	
	private static AtomicLong inboxCtr = new AtomicLong(1);
	
	/**
	 * This constructor is called by the outside JVM. 
	 * For debugging purposes, this might indeed also be a JVM that fulfills
	 * the duties of the enclave (i.e., a JVM that runs outside of the enclave
	 * but does what the enclave is supposed to do). 
	 * 
	 * @param file
	 * @param size
	 */
	private ShmCommunicationManager(String file, int size) {
		if (!initialized) {
			long[] handles = RingBuffLibWrapper.init_shm(file, size);

			if (SgxSettings.IS_ENCLAVE() && !SgxSettings.DEBUG_IS_ENCLAVE_REAL()) {
				// debugging case: switch producer and consumer,
				// since this instance of the code is actually the enclave side of things
				this.reader = new RingBuffConsumer(new MappedDataBuffer(handles[1], size), Serialization.serializer);
				this.writer = new RingBuffProducer(new MappedDataBuffer(handles[0], size), Serialization.serializer);
			} else {
				// default case
				this.reader = new RingBuffConsumer(new MappedDataBuffer(handles[0], size), Serialization.serializer);
				this.writer = new RingBuffProducer(new MappedDataBuffer(handles[1], size), Serialization.serializer);
			}

			MappedDataBufferManager.init(new MappedDataBuffer(handles[2], size));
			initialized = true;
		}
	}

	/**
	 * This constructor is called by the enclave, which is provided with
	 * corresponding pointers to shared memory by sgx-lkl.
	 * 
	 * @param writeBuff pointer to the memory to write to
	 * @param readBuff pointer to the memory to read from
	 * @param commonBuff pointer to the memory that can be used arbitrarily
	 * @param size the size of each of those memory regions
	 */
	private ShmCommunicationManager(long writeBuff, long readBuff, long commonBuff, int size) {
		if (!initialized) {
			this.reader = new RingBuffConsumer(new MappedDataBuffer(readBuff, size), Serialization.serializer);
			this.writer = new RingBuffProducer(new MappedDataBuffer(writeBuff, size), Serialization.serializer);
			MappedDataBufferManager.init(new MappedDataBuffer(commonBuff, size));
			initialized = true;
		}
	}

	public static <T> ShmCommunicationManager<T> create(String file, int size) {
		return new ShmCommunicationManager<T>(file, size);
	}

	public static <T> ShmCommunicationManager<T> create(long writeBuff, long readBuff, long commonBuff, int size) {
		return new ShmCommunicationManager<T>(writeBuff, readBuff, commonBuff, size);
	}

	public static ShmCommunicator newShmCommunicator() {
		return newShmCommunicator(true);
	}

	public static ShmCommunicator newShmCommunicator(boolean doConnect) {
		BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
		long myport;
		myport = inboxCtr.getAndIncrement();
		inboxes.put(myport, inbox);

		return new ShmCommunicator(myport, inbox, doConnect);
	}

	/**
	 * Waits for a new incoming connections. This call blocks until
	 * a new connection is actually made. Once a connection is made,
	 * this method returns a {@link ShmCommunicator} object that
	 * represents this new connection. 
	 * This is conceptually similar to accept() on TCP sockets.
	 * 
	 * @return a {@link ShmCommunicator} representing the accepted connection
	 */
	public static ShmCommunicator accept() {
		ShmCommunicator result = null;
		do {
			try {
				result = accepted.take();
			} catch (InterruptedException e) {
				result = null;
				e.printStackTrace();
			}
		} while (result == null);
		return result;
	}

	/**
	 * Writes to the outside.
	 * 
	 * @param o the object to write
	 * @return whether the write was successful
	 */
	public static void write(Object o, long theirPort) {
		write(new ShmMessage(EShmMessageType.REGULAR, o, theirPort));
	}

	public static void write(ShmMessage m) {
		writer.writeShmMessage(m);
	}
	
	public static void close(ShmCommunicator com) {
		inboxes.remove(com.getMyPort());
	}

	@Override
	public T call() throws Exception {
		ShmMessage msg = null;
		while (true) {
			msg = reader.readShmMessage();

			if (msg.getPort() == 0) {
				switch (msg.getType()) {
				case NEW_CONNECTION:
					BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
					long myport;
					myport = inboxCtr.getAndIncrement();
					inboxes.put(myport, inbox);
					write(new ShmMessage(EShmMessageType.ACCEPTED_CONNECTION, myport, (long) msg.getMsg()));
					accepted.put(new ShmCommunicator(myport, (long) msg.getMsg(), inbox));
					break;

				default:
					break;
				}
			} else {
				BlockingQueue<Object> inbox;
				inbox = inboxes.get(msg.getPort());
				inbox.add(msg.getMsg());
			}
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
