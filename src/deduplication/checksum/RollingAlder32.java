package deduplication.checksum;

import java.util.HashSet;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class RollingAlder32 {
	
	/**
	 * Rolling checksum with Alder32 algorithm
	 * @param data Data to rolling
	 * @param offset Initial position in the byte array
	 * @param window Amount of bytes in array to compute a hash 
	 */
	public static HashSet<Long> rollingIn(byte[] data, int offset, int window) {				
		Checksum checksum = new Adler32();
		HashSet<Long> hashes = new HashSet<Long>();
		
		if(window > data.length) {
			checksum.update(data, offset, data.length);
			hashes.add(checksum.getValue());
			checksum.reset();
		} else {
			while (offset <= data.length - window) {				
				//byte[] partial = Arrays.copyOfRange(data, offset, offset + window);
				//System.out.println((new String(partial)) + " " + Hashing.getAlder32(partial));
				System.out.println(offset + " of " + data.length);
				
				checksum.update(data, offset, window);
				hashes.add(checksum.getValue());
				checksum.reset();
				offset++;
			}
		}
		
		return hashes;
	}
	
}
