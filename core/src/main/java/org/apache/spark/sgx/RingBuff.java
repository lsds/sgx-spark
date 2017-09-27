package org.apache.spark.sgx;

import java.io.IOException;

public class RingBuff {
        private long handle;

        static{
                System.loadLibrary("ringbuff");
        }

        public RingBuff(long handle) {
                this.handle = handle;
        }

        private native boolean write_msg(long handle, byte[] msg, int len);
        private native byte[] read_msg(long handle);

        public boolean write(Object o) {
        	boolean result = false;
			try {
                byte[] b;
                b = Serialization.serialize(o);
                result = write_msg(handle, b, b.length);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return result;
        }

        public Object read() {
        	Object obj = null;
        	try {
				obj = Serialization.deserialize(read_msg(handle));
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
        	return obj;
        }
}

