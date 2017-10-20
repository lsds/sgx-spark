package org.apache.spark.sgx;

public abstract class SgxCommunicationInterface {
	
	/**
	 * Send one object.
	 * @param o the object to send
	 */
	public abstract void sendOne(Object o);
	
	/**
	 * Receive one object.
	 * @return the received object
	 */
	public abstract Object recvOne();
	
	/**
	 * Send one object and wait for the result of type T.
	 * @param o the object to send
	 * @return the result object of type T
	 */
	public abstract <T> T sendRecv(Object o);
	
	public abstract void close();
	
	public abstract Object read();
	
	public abstract void write(Object o);
}
