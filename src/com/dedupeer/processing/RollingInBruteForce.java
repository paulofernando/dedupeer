package com.dedupeer.processing;

import java.util.HashSet;

import org.apache.log4j.Logger;

import com.dedupeer.checksum.Hashing;
import com.dedupeer.checksum.RollingAdler32;


/**
 * Responsible to identify a specific chunk into the block of data using brute force
 * @author Paulo Fernando
 * @deprecated
 */
public class RollingInBruteForce {
	
	private static final Logger log = Logger.getLogger(RollingInBruteForce.class);
	
	/**
	 * Identifies if the chunk is duplicated in the file
	 * @param block Block of the data 
	 * @param chunk Chunk to find in the block of file
	 */
	public static void duplicationIdentification(byte[] block, byte[] chunk) {
		
		if(block.length < chunk.length) {
			log.debug("ERROR!");
			return;
		}
		
		HashSet<Long> rollingHashes = RollingAdler32.rollingIn(block, 0, chunk.length);
		long chunkHash = Hashing.getAlder32(chunk);
					
		log.debug("--- \nChunk: " + (new String(chunk)) + "\nHash: " + chunkHash);
		
		if(rollingHashes.contains(chunkHash)) {
			log.debug("Pattern found!");
		} else {
			log.debug("Pattern not found!");
		}
		
	}
}
