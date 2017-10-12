package org.apache.spark.sgx;

public class OutsideCommunicator {
    private RingBuff encToOut;
    private RingBuff outToEnc;
    
    public OutsideCommunicator(String file, int size) {
    	long[] handles = RingBuffLibWrapper.init_shm(file, size);
    	this.encToOut = new RingBuff(handles[0], true);
    	this.outToEnc = new RingBuff(handles[1], true);
    }

	public boolean writeToEnclave(Object o) {
    	return this.outToEnc.write(o);
	}
	
	public Object readFromEnclave() {
    	return this.encToOut.read();
	}
}