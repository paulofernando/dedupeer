package deduplication.checksum;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.springframework.util.Assert;

/**
 * @deprecated
 */
public class RollingChecksumOlder {
	
	private static final Logger log = Logger.getLogger(RollingChecksumOlder.class);	
	private byte[] data;	
	private InputStream stream;	
	private File f;	
	private boolean eofReached = false;	
	private int readOffset = 0;	
	private int blockSize;	
	private int index = -1;	
	private long a = 0;	
	private long b = 0;	
	private static long M = (long) Math.pow(2, 16);
	
	public RollingChecksumOlder(File f, int blockSize) {
		this.f = f;
		this.blockSize = Math.min((int) f.length(), blockSize);
		this.data = new byte[(int) f.length()]; //Maximum filesize: 2,147,483,647
		try {
			this.stream = new FileInputStream(f);
		} catch (FileNotFoundException e) {
			log.info(String.format("Failed to access file [%s]", f.getAbsolutePath()));
		}
	}
	
	public RollingChecksumOlder(String string, int blockSize) {
		Assert.notNull(string);
		this.data = string.getBytes();
		this.blockSize = Math.min(data.length, blockSize);
	}
	
	public RollingChecksumOlder(byte[] data, int blockSize) {
		this.data = data;
		this.blockSize = Math.min(data.length, blockSize);
	}
	
	public final void reset() {
		index = -1;
		a = 0;
		b = 0;
	}
	
	public boolean next() {
		if (index < data.length - blockSize) {			
			//If pulling data off a stream, get another chunk of data here
			if (stream != null && !eofReached) {
				if (log.isDebugEnabled())
					log.debug("Buffering more data...");
					
				try {
					int bytesRead = stream.read(data, readOffset, blockSize);
					if (bytesRead >= 0)
						readOffset += bytesRead;
					else {
						eofReached = true;
						stream.close();
					}
				} catch (IOException e) {
					System.out.println(String.format("Failed to read or close file [%s]", f.getAbsolutePath()));
				}
			}			
			index++;
			return true;
		} else {
			return false;
		}
	}
	
	public static final Long sum(byte[] chunk) {
		long a = 0, b = 0;
		
		for (int i=0; i<chunk.length; i++) {
			a += (long) chunk[i];
			b += (chunk.length-i) * (long) chunk[i];
		}
		
		a = a % M;
		b = b % M;
		
		long checksum = a + M * b;
		return checksum;
	}
	
	public static final Long sum(String str, int start, int end) {
		
		String sub = str.substring(start, Math.min(str.length(), end));
		long checksum = sum(sub.getBytes());
		
		if (log.isDebugEnabled())
			log.debug("sum("+sub+"): " + Long.toHexString(checksum));
		
		return checksum;		
	}
	
	public long weak() {
		
		long checksum;
		
		if (index == 0) { // s(l, k) = a(l, k) + M * b(l, k)
			
			byte[] firstBlock = new byte[Math.min(blockSize, data.length)];
			System.arraycopy(data, 0, firstBlock, 0, Math.min(blockSize, data.length));
			
			//Initialize alpha and beta summations
			for (int i=0; i<firstBlock.length; i++) {
				a += (long) firstBlock[i];
				b += (blockSize-i) * (long) firstBlock[i];
			}
			
			a = a % M;
			b = b % M;
			
			checksum = a + M * b;
			
			if (log.isTraceEnabled()) {
				StringBuilder hex = new StringBuilder();
				for (int i=0; i<firstBlock.length; i++)
					hex.append(Long.toHexString((long) firstBlock[i]));
				log.trace("weak(block:"+hex.toString()+"): " + Long.toHexString(checksum));
			}			
		} else {			
			int lastByteAt = index - 1;
			int nextByteAt = index + blockSize - 1;			
			byte lastByte = data[lastByteAt];
			byte nextByte = data[nextByteAt];
			
			a = (a - (long) lastByte + (long) nextByte) % M;
			b = (b - blockSize * (long) lastByte + a) % M;
			
			checksum = a + M * b;
			
			if (log.isTraceEnabled()) {
				log.trace("weak(lastByte:"+Long.toHexString((long) lastByte)+", nextByte:"+Long.toHexString((long) nextByte) + "): " + Long.toHexString(checksum));
			}			
		}		
		return checksum;
	}
}
