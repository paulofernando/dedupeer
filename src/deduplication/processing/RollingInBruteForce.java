package deduplication.processing;

import java.util.HashSet;

import deduplication.checksum.Hashing;
import deduplication.checksum.RollingAdler32;

/**
 * Responsible to identify a specific chunk into the block of data using brute force
 * @author Paulo Fernando
 *
 */
public class RollingInBruteForce {
	
	/**
	 * Identifies if the chunk is duplicated in the file
	 * @param block Block of the data 
	 * @param chunk Chunk to find in the block of file
	 */
	public static void duplicationIdentification(byte[] block, byte[] chunk) {
		
		if(block.length < chunk.length) {
			System.out.println("ERROR!");
			return;
		}
		
		HashSet<Long> rollingHashes = RollingAdler32.rollingIn(block, 0, chunk.length);
		long chunkHash = Hashing.getAlder32(chunk);
					
		System.out.println("--------- \nChunk: " + (new String(chunk)) + "\nHash: " + chunkHash);
		
		if(rollingHashes.contains(chunkHash)) {
			System.out.println("achou!");
		} else {
			System.out.println("não achou!");
		}
		
	}
}
