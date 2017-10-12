package org.apache.spark.sgx;

public class OutsideCommunicator {
    private RingBuff encToOut;
    private RingBuff outToEnc;
    
    public OutsideCommunicator(String file, int size) {
    	long[] handles = RingBuffLibWrapper.init_shm(file, size);
    	this.encToOut = new RingBuff(handles[0]);
    	this.outToEnc = new RingBuff(handles[1]);
    }

	public void writeToEnclave(Object o) {
    	this.outToEnc.write(o);
	}
	
	public Object readFromEnclave() {
    	return this.encToOut.read();
	}
}