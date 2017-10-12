package org.apache.spark.sgx;

public class EnclaveCommunicator {
    private RingBuff encToOut;
    private RingBuff outToEnc;
    
	public EnclaveCommunicator(long encToOut, long outToEnc) {
    	this.encToOut = new RingBuff(encToOut);
    	this.outToEnc = new RingBuff(outToEnc);
	}

	public void writeToOutside(Object o) {
    	this.encToOut.write(o);
	}
	
	public Object readFromOutside() {
    	return this.outToEnc.read();
	}
}