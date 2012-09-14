package deduplication.processing;

import java.util.ArrayList;
import java.util.Arrays;

import deduplication.checksum.RollingChecksumOlder;

public class EagleEye {
	
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk}
	 * @param file File where the block will be searched
	 * @param chunk Data block to find in {@code file}
	 * @return Indexes on the {@code file} where the pattern matches
	 */
	public static ArrayList<Integer> searchDuplication(byte[] file, byte[] chunk) {
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		
		Long hash = RollingChecksumOlder.sum(chunk);
		RollingChecksumOlder checksum = new RollingChecksumOlder(file, chunk.length);
		
		int i = 0;
		while (checksum.next()) {
			long cs = checksum.weak();			
			if(cs == hash) {				
				System.out.println("\nAchou! [index = " + i +"]");
				indexes.add(i);
				System.out.println(cs);
			}
			i++;
		}
		
		if(indexes.size() == 0) System.out.println("Bloco não duplicado");
		
		return indexes;
	}
	
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk}
	 * @param file File where the block will be searched
	 * @param hash Hash computed of a chunk
	 * @param sizeOfChunk Size of the chunk from which the {@code hash} was computed
	 * @return index on the {@code file} where the pattern matches. -1 if not found it.
	 */
	public static int searchDuplication(byte[] file, long hash, int sizeOfChunk) {					
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
