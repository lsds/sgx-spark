package org.apache.spark.sgx.shm;

import java.util.concurrent.BlockingQueue;

import org.apache.spark.sgx.SgxCommunicationInterface;

public class ShmCommunicator extends SgxCommunicationInterface {
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

	public Object read() {
		Object result = null;
		do {
			try {
				result = inbox.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while (result == null);

		return result;
	}

	@Override
	public void sendOne(Object o) {
		write(o);
	}

	@Override
	public Object recvOne() {
		return read();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T sendRecv(Object o) {
		sendOne(o);
		return (T) recvOne();
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
