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
		ShmCommunicationManager.write(msg);
	}

	public void write(Object o) {
		ShmCommunicationManager.write(o, theirPort);
	}

	public Object read() {
		Object result = null;
		do {
			try {
				result = inbox.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				result = null;
			}
		} while (result == null);
		return result;
	}

	@Override
	public void close() {
		inbox.clear();
		ShmCommunicationManager.close(this);
	}

	public String toString() {
		return getClass().getSimpleName() + "(myPort=" + myPort + ", theirPort=" + theirPort + ")";
	}
}
