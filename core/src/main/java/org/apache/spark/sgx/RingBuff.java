package org.apache.spark.sgx;

import java.io.IOException;

public class RingBuff {
        private long handle;

        public RingBuff(long handle) {
        	this.handle = handle;
        }
        
        public boolean write(Object o) {        	
        	boolean result = false;
			try {
                byte[] b;
                b = Serialization.serialize(o);
                result = RingBuffLibWrapper.write_msg(handle, b);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return result;
        }

        public Object read() {
        	Object obj = null;
        	try {
				obj = Serialization.deserialize(RingBuffLibWrapper.read_msg(handle));
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
        	return obj;
        }
}
