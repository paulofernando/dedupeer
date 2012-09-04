package deduplication.checksum;

import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class RollingAlder32 {
	
	public static void rollingIn(byte[] data, long offset, int window) {
		
		
		Checksum checksum = new Adler32();        
        checksum.update(data, 0, bytes.length);
       
        /*
         * Get the generated checksum using
         * getValue method of Adler32 class.
         */
        long lngChecksum = checksum.getValue();
		
		
		System.out.println();
	}
	
}
