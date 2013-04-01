package com.dedupeer.processing;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.dedupeer.checksum.Checksum32;
import com.dedupeer.checksum.RollingChecksumOlder;


/** 
 * @author Paulo Fernando (pf@paulofernando.net.br)
 * @deprecated
 */
public class EagleEye {	
	
	private static final Logger log = Logger.getLogger(EagleEye.class);
	
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk}
	 * @param file File where the block will be searched
	 * @param chunk Data block to find in {@code file}
	 * @return index on the {@code file} where the pattern matches. -1 means the pattern not found
	 */
	public static int searchDuplication(byte[] file, byte[] chunk) {
		Checksum32 chunkC32 = new Checksum32();
		chunkC32.check(chunk, 0, chunk.length);
		int chunkHash = chunkC32.getValue();		
		long time = System.currentTimeMillis();		
		int index = 0;
		
		Checksum32 c32 = new Checksum32();
		c32.check(file, 0, chunk.length);
		int hash;
		
		hash = c32.getValue();
		if(chunkHash == hash) {
			log.debug("Found it! [hash = " + hash + "] and [index = " + index + "]");
			return index;
		}			
		index++;
		
		while(index < file.length - chunk.length) {
			c32.roll(file[index]);
			hash = c32.getValue();
			if(chunkHash == hash) {
				log.debug("Found it! [hash = " + hash + "] and [index = " + index + "]");				
				return index;
			}			
			index++;
		}
		
		log.debug("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
		return -1;
	}
	
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk}
	 * @param file File where the block will be searched
	 * @param chunkHash Hash computed of a chunk
	 * @param offset Position of the last byte of new chunk to search
	 * @param sizeOfChunk Size of the chunk from which the {@code hash} was computed
	 * @return index on the {@code file} where the pattern matches. -1 if not found it.
	 */
	public static int searchDuplication(byte[] file, int chunkHash, int offset, int sizeOfChunk) {	
		int index = offset;
		
		Checksum32 c32 = new Checksum32();
		c32.check(file, offset, sizeOfChunk);
		int hash;
		
		hash = c32.getValue();
		if(chunkHash == hash) {			
			log.debug("Found it! [hash = " + hash + "] and [index = " + index + "] *");
			return index;
		}			
		index++;
		
		while(index <= file.length - sizeOfChunk) {
			c32.roll(file[index]);
			hash = c32.getValue();
			
			if(chunkHash == hash) {
				index -= (sizeOfChunk - 1); //the index informed to roll() is the index of the last byte
				log.debug("Found it! [hash = " + hash + "] and [index = " + index + "]");				
				return index;
			}			
			index++;
		}
		log.debug("-> Pattern not found!");	
		return -1;
	}
		
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk}
	 * @param file File where the block will be searched
	 * @param chunk Data block to find in {@code file}
	 * @return Indexes on the {@code file} where the pattern matches
	 */
	@Deprecated
	public static ArrayList<Integer> searchDuplicationWithChecksumOlder(byte[] file, byte[] chunk) {
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		
		Long hash = RollingChecksumOlder.sum(chunk);
		RollingChecksumOlder checksum = new RollingChecksumOlder(file, chunk.length);
		
		int i = 0;
		while (checksum.next()) {
			long cs = checksum.weak();
			if(cs == hash) {				
				log.debug("\nFound! [index = " + i +"]");
				indexes.add(i);
				log.debug(cs);
			}
			i++;
		}
		
		if(indexes.size() == 0) {
			log.debug("Duplicated block");
		}
		
		return indexes;
	}
	
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk}
	 * @param file File where the block will be searched
	 * @param hash Hash computed of a chunk
	 * @param sizeOfChunk Size of the chunk from which the {@code hash} was computed
	 * @return index on the {@code file} where the pattern matches. -1 if not found it.
	 */
	@Deprecated
	public static int searchDuplicationWithChecksumOlder(byte[] file, long hash, int sizeOfChunk) {					
		RollingChecksumOlder checksum = new RollingChecksumOlder(file, sizeOfChunk);
		
		int i = 0;
		while (checksum.next()) {
			long cs = checksum.weak();			
			if(cs == hash) {
				return i;
			}
			i++;
		}
				
		return -1;
	}
	
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk} without checksum
	 * @param file File where the block will be searched
	 * @param hash Hash computed of a chunk
	 * @param start Initial byte to search
	 * @param sizeOfChunk Size of the chunk from which the {@code hash} was computed
	 * @return index on the {@code file} where the pattern matches. -1 if not found it.
	 */
	@Deprecated
	public static int searchDuplicationWithoutRollingChecksum(byte[] file, int start, long hash, int sizeOfChunk) {
		file = Arrays.copyOfRange(file, start, file.length);
		RollingChecksumOlder checksum = new RollingChecksumOlder(file, sizeOfChunk);
		
		int i = 0;
		while (checksum.next()) {
			if(i + sizeOfChunk < file.length) {
				long cs = RollingChecksumOlder.sum(Arrays.copyOfRange(file, i, i + sizeOfChunk));
				if(cs == hash) {
					return i;
				}
				
				//FIXME Byte did not jump one by one. 
				i += sizeOfChunk;
			}
		}				
		return -1;
	}
}
