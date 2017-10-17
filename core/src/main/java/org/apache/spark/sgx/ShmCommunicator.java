package org.apache.spark.sgx;

import java.util.concurrent.BlockingQueue;

public class ShmCommunicator implements SgxCommunicationInterface {
	private long myPort;
	private long theirPort;
	private ShmCommunicationManager<?> manager;
	private BlockingQueue<Object> inbox;

	ShmCommunicator(long myPort, long theirPort, BlockingQueue<Object> inbox, ShmCommunicationManager<?> manager) {
		this.myPort = myPort;
		this.theirPort = theirPort;
		this.inbox = inbox;
		this.manager = manager;
	}

	ShmCommunicator(long myPort, BlockingQueue<Object> inbox, ShmCommunicationManager<?> manager) {
		this.myPort = myPort;
		this.inbox = inbox;
		this.manager = manager;

		write(new ShmMessage(EShmMessageType.NEW_CONNECTION, myPort, 0));

		Object reply;
		try {
			reply = inbox.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		this.theirPort = (int) reply;
	}

	public long getMyPort() {
		return myPort;
	}

	private boolean write(ShmMessage msg) {
		return manager.write(msg);
	}

	private boolean write(Object o) {
		return manager.write(o, theirPort);
	}

	private Object read() {
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
		return ((ShmMessage) read()).getMsg();
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
		manager.close(this);
	}

	public String toString() {
		return this.getClass().getSimpleName() + "(myPort=" + myPort + ", theirPort=" + theirPort + ")";
	}
}
