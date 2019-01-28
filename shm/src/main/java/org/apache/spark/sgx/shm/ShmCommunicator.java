package org.apache.spark.sgx.shm;

import java.util.concurrent.BlockingQueue;

import org.apache.spark.sgx.SgxCommunicator;

public class ShmCommunicator extends SgxCommunicator {
	private final Long myPort;
	private Long theirPort;
	private final BlockingQueue<Object> inbox;

	ShmCommunicator(long myPort, long theirPort, BlockingQueue<Object> inbox) {
	try {
		throw new Exception ("Create new ShmCommunicator1 " + myPort + ", " + theirPort + ", " + inbox);
	} catch (Exception e) {
		e.printStackTrace();
	}

		this.myPort = myPort;
		this.theirPort = theirPort;
		this.inbox = inbox;
	}

	ShmCommunicator(long myPort, BlockingQueue<Object> inbox, boolean doConnect) {
		try {
			throw new Exception ("Create new ShmCommunicator2 " + myPort + ", " + inbox + ", " + doConnect);
		} catch (Exception e) {
			e.printStackTrace();
		}

		this.myPort = myPort;
		this.inbox = inbox;

		if (doConnect) {
			write(new ShmMessage(EShmMessageType.NEW_CONNECTION, myPort, 0L));
	
			Object reply;
			try {
				reply = inbox.take();
			} catch (InterruptedException e) {
				System.out.println(e);
				throw new RuntimeException(e);
			}
	
			this.theirPort = (long) reply;
		}
	}
	
	public ShmCommunicator connect(long theirPort) {
		if (this.theirPort == null) {
			this.theirPort = theirPort;
		} else {
			throw new RuntimeException(this + " was already connected to " + theirPort);
		}
		return this;
	}

	public long getMyPort() {
		return myPort;
	}

	public long getTheirPort() {
		return theirPort;
	}

	private void write(ShmMessage msg) {
		ShmCommunicationManager.get().write(msg);
	}

	public void write(Object o) {
		ShmCommunicationManager.get().write(o, theirPort);
	}

	public Object read() {
	try {
		throw new Exception("ShmCommunicator.read() " + toString());
	} catch (Exception e) {
		e.printStackTrace();
	}

		Object result = null;
		do {
			try {
				System.err.println("ShmCommunicator.read() 2 on port " + myPort);
				result = inbox.take();
				System.err.println("ShmCommunicator.read() 3 on port " + myPort);
			} catch (InterruptedException e) {
				e.printStackTrace();
				result = null;
			}
		} while (result == null);
		System.err.println("ShmCommunicator.read() 4 on port " + myPort + ": msg is " + result.toString());
		return result;
	}

	@Override
	public void close() {
		inbox.clear();
		ShmCommunicationManager.get().close(this);
	}

	public String toString() {
		return getClass().getSimpleName() + "(myPort=" + myPort + ", theirPort=" + theirPort + ")";
	}
}
