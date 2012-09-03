package test.java.net.collegeman.rsync;

import junit.framework.TestCase;
import net.collegeman.rsync.checksum.RollingChecksum;

public class ChecksumTests extends TestCase {
	
	public void testRollingChecksumWithString() {
		
		String phrase = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
		
		int blockSize = 3000;
		
		RollingChecksum checksum = new RollingChecksum(phrase, blockSize);
		
		int i=0;
		while (checksum.next()) {
			long c = checksum.weak();
			String expected = Long.toHexString(RollingChecksum.sum(phrase, i, i+blockSize));
			String found = Long.toHexString(c);
			assertTrue("checksum " + i + ": expecting " + expected + ", found: " + found, found.endsWith(expected));
			i++;
		}
		
		assertEquals("didn't get enough checksums...", Math.max(1, phrase.length()-blockSize+1), i);
		
	}
	
	public void testRollingChecksumWithFile() {
		
		
		
	}

}
