package org.apache.spark.sgx;

import java.util.concurrent.BlockingQueue;

public class ShmCommunicator implements SgxCommunicationInterface {
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
			RuntimeException e = new RuntimeException(this + " was already connected to " + theirPort);
			System.out.println(e);
			throw e;
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
		System.out.println("Sending E");
	}

	private void write(Object o) {
		ShmCommunicationManager.get().write(o, theirPort);
		System.out.println("Sending D");
	}

	private Object read() {
		System.out.println("waiting 2");
		Object result = null;
		System.out.println("waiting 3");
		do {
			try {
				System.out.println("waiting 4");
				result = inbox.take();
				System.out.println(this + " taken from inbox: " + result);
			} catch (InterruptedException e) {
				System.out.println("waiting 5");
				e.printStackTrace();
				System.out.println("waiting 6");
			}
			System.out.println("waiting 7");
		} while (result == null);
		System.out.println("waiting 8");

		return result;
	}

	@Override
	public void sendOne(Object o) {
		write(o);
		System.out.println("Sending F");
	}

	@Override
	public Object recvOne() {
//		return ((ShmMessage) read()).getMsg();
		System.out.println("waiting 1");
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
