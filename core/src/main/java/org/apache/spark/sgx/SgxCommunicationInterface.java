package org.apache.spark.sgx;

public interface SgxCommunicationInterface {
	
	/**
	 * Send one object.
	 * @param o the object to send
	 */
	public void sendOne(Object o);
	
	/**
	 * Receive one object.
	 * @return the received object
	 */
	public Object recvOne();
	
	/**
	 * Send one object and wait for the result of type T.
	 * @param o the object to send
	 * @return the result object of type T
	 */
	public <T> T sendRecv(Object o);
	
//	def sendMany(it: Iterator[Any]): Unit = {
//	it.foreach {
//		x => sendOne(x)
//	}
//	sendOne(MsgDone)
//}
//
//def recvMany(): Iterator[Any] = {
//	var list = new ListBuffer[Any]()
//	while(recvOne() match {
//		case MsgDone => false
//		case x: Any =>
//			list += x
//			true
//	}){}
//	list.iterator
//}	
	
	void close();
}
