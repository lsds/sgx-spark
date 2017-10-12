//package org.apache.spark.sgx;
//
//class EnclaveOutsideCommunicator {
//    private RingBuff encToOut;
//    private RingBuff outToEnc;
//    
//    protected EnclaveOutsideCommunicator(String file, int size) {
//    	long[] handles = RingBuffLibWrapper.init_shm(file, size);
//    	this.encToOut = new RingBuff(handles[0]);
//    	this.outToEnc = new RingBuff(handles[1]);
//    }
//    
//    protected EnclaveOutsideCommunicator(long encToOut, long outToEnc) {
//    	this.encToOut = new RingBuff(encToOut);
//    	this.outToEnc = new RingBuff(outToEnc);
//    }
//    
//    protected void writeToEnclave(Object o) {
//    	this.outToEnc.write(o);
//    }
//    
//    protected Object readFromOutside() {
//    	return this.outToEnc.read();
//    }
//    
//    protected void writeToOutside(Object o) {
//    	this.encToOut.write(o);
//    }
//    
//    protected Object readFromEnclave() {
//    	return this.encToOut.read();
//    }
//}
//
