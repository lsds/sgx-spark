package org.apache.spark.sgx.shm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.spark.sgx.Serialization;
import org.apache.spark.sgx.SgxSettings;

/**
 * This class is to be used by the enclave to communicate with the outside.
 * 
 * @author Florian Kelbert
 *
 */
public final class ShmCommunicationManager<T> implements Callable<T> {
	
	private RingBuffConsumer reader;
	private RingBuffProducer writer;

	private final Object lockWriteBuff = new Object();
	private final Object lockReadBuff = new Object();
	private final Object lockInboxes = new Object();
	private final static Object lockInstance = new Object();

	private Map<Long, BlockingQueue<Object>> inboxes = new HashMap<>();
	private BlockingQueue<ShmCommunicator> accepted = new LinkedBlockingQueue<>();
	
	private long inboxCtr = 1;
	
	private static ShmCommunicationManager<?> _instance = null;
	
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
		long[] handles = RingBuffLibWrapper.init_shm(file, size);
		
		if (SgxSettings.IS_ENCLAVE() && !SgxSettings.DEBUG_IS_ENCLAVE_REAL()) {
			// debugging case: switch producer and consumer,
			// since this instance of the code is actually the enclave side of things
			this.reader = new RingBuffConsumer(new MappedDataBuffer(handles[1], size), Serialization.serializer);
			this.writer = new RingBuffProducer(new MappedDataBuffer(handles[0], size), Serialization.serializer);
		}
		else {
			// default case
			this.reader = new RingBuffConsumer(new MappedDataBuffer(handles[0], size), Serialization.serializer);
			this.writer = new RingBuffProducer(new MappedDataBuffer(handles[1], size), Serialization.serializer);
		}

		MappedDataBufferManager.init(new MappedDataBuffer(handles[2], size));
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
		this.reader = new RingBuffConsumer(new MappedDataBuffer(readBuff, size), Serialization.serializer);
		this.writer = new RingBuffProducer(new MappedDataBuffer(writeBuff, size), Serialization.serializer);
		MappedDataBufferManager.init(new MappedDataBuffer(commonBuff, size));
	}
	
	@SuppressWarnings("unchecked")
	public static <T> ShmCommunicationManager<T> create(String file, int size) {
		synchronized(lockInstance) {
			if (_instance == null) {
				_instance = new ShmCommunicationManager<T>(file, size);
			}
		}
		return (ShmCommunicationManager<T>) _instance;
	}	
	
	@SuppressWarnings("unchecked")
	public static <T> ShmCommunicationManager<T> create(long writeBuff, long readBuff, long commonBuff, int size) {
		synchronized(lockInstance) {
			if (_instance == null) {
				_instance = new ShmCommunicationManager<T>(writeBuff, readBuff, commonBuff, size);
			}
		}
		return (ShmCommunicationManager<T>) _instance;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> ShmCommunicationManager<T> get() {
		if (_instance == null) {
			throw new RuntimeException("ShmCommunicationManager was not instantiated.");
		}
		return (ShmCommunicationManager<T>) _instance;
	}

	public ShmCommunicator newShmCommunicator() {
		return newShmCommunicator(true);
	}

	public ShmCommunicator newShmCommunicator(boolean doConnect) {
		BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
		long myport;
		synchronized (lockInboxes) {
			myport = inboxCtr++;
			inboxes.put(myport, inbox);
		}

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
	public ShmCommunicator accept() {
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
	void write(Object o, long theirPort) {
		write(new ShmMessage(EShmMessageType.REGULAR, o, theirPort));
	}

	void write(ShmMessage m) {
		synchronized (lockWriteBuff) {
			writer.write(m);
		}
	}
	
	void close(ShmCommunicator com) {
		inboxes.remove(com.getMyPort());
	}

	@Override
	public T call() throws Exception {
		ShmMessage msg = null;
		while (true) {
			synchronized (lockReadBuff) {
				msg = ((ShmMessage) reader.read());
			}

			if (msg.getPort() == 0) {
				switch (msg.getType()) {
				case NEW_CONNECTION:
					BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
					long myport;
					synchronized (lockInboxes) {
						myport = inboxCtr++;
						inboxes.put(myport, inbox);
					}
					accepted.put(new ShmCommunicator(myport, (long) msg.getMsg(), inbox));
					write(new ShmMessage(EShmMessageType.ACCEPTED_CONNECTION, myport, (long) msg.getMsg()));
					break;

				default:
					break;
				}
			} else {
				BlockingQueue<Object> inbox;
				synchronized (lockInboxes) {
					inbox = inboxes.get(msg.getPort());
				}
				inbox.add(msg.getMsg());
			}
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}