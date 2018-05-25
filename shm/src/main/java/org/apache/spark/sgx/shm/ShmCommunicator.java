package org.apache.spark.sgx.shm;

import java.util.concurrent.BlockingQueue;

import org.apache.spark.sgx.SgxCommunicator;

public class ShmCommunicator extends SgxCommunicator {
	private final Long myPort;
	private Long theirPort;
	private final BlockingQueue<Object> inbox;

	ShmCommunicator(long myPort, long theirPort, BlockingQueue<Object> inbox) {
		this.myPort = myPort;
		this.theirPort = theirPort;
		this.inbox = inbox;
	}

	ShmCommunicator(long myPort, BlockingQueue<Object> inbox, boolean doConnect) {
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
	
	private Object t() throws InterruptedException {
		return inbox.take();
	}
	
	private Object t1() {
		Object result = null;
		try {
			result = t();
		} catch (InterruptedException e) {
			e.printStackTrace();
			result = null;
		}
		return result;
	}
	
	private Object t2() {
		Object result = null;
		do {
			result = t1();
		} while (result == null);
		return result;
	}

	public Object read() {
		Object result = t2();
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
