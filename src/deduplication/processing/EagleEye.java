package deduplication.processing;

import java.util.ArrayList;

import deduplication.checksum.RollingChecksum;

public class EagleEye {
	
	/**
	 * Try find a data block in {@code file} with same bytes as the {@code chunk}
	 * @param file File where the block will be searched
	 * @param chunk Data block to find in {@code file}
	 * @return Indexes on the {@code file} where the pattern matches
	 */
	public static ArrayList<Integer> searchDuplication(byte[] file, byte[] chunk) {
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		
		Long hash = RollingChecksum.sum(chunk);
		RollingChecksum checksum = new RollingChecksum(file, chunk.length);
		
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
}
