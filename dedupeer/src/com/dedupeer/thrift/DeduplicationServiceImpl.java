package com.dedupeer.thrift;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.dedupeer.checksum.Checksum32;
import com.dedupeer.utils.FileUtils;

public class DeduplicationServiceImpl implements DeduplicationService.Iface {

	private static final Logger log = Logger.getLogger(DeduplicationServiceImpl.class);
	private FileUtils fileUtils = new FileUtils();
	private byte[] newchunk;
	
	/** Indicates if the hashes of all chunks must be calculated or if only hashes of chunks with default size.
	 * Drawback if false: do not deduplicate whole identical file because do not compares all chunks */
	private boolean calculateAllHashes = fileUtils.getPropertiesLoader().getProperties().getProperty("calculate.all.hashes").equalsIgnoreCase("true");
		
	@Override
	public Map<Long, Chunk> deduplicate(
			Map<Integer, Map<String, ChunkIDs>> chunksInfo, String pathOfFile,
			int chunkSizeInBytes, int bytesToLoadByTime,
			HashingAlgorithm hashingAlgorithm) throws TException {
		long time = System.currentTimeMillis();
		log.info("\n[Deduplicating...]");
		
		long timeToRetrieve = System.currentTimeMillis();
				
		String newFileID = String.valueOf(System.currentTimeMillis());
		HashMap<Long, Chunk> newFileChunks = new HashMap<Long, Chunk>();
		HashMap<Long, Chunk> resultChunks = new HashMap<Long, Chunk>();
		int chunk_number = 0;
		Checksum32 c32 = new Checksum32();
		
		long offset = 0;
		
		ByteBuffer buffer = ByteBuffer.allocate(chunkSizeInBytes);
		/** Current index in the divided byte array */
		int localIndex = 0;
		/** Index in the whole file */
		long globalIndex = 0;
		long referencesCount = 0;
		
		File file = new File(pathOfFile);
		
		if(bytesToLoadByTime > file.length()) {
			bytesToLoadByTime = (int)file.length();
		} else {
			bytesToLoadByTime = (bytesToLoadByTime % chunkSizeInBytes == 0 ? bytesToLoadByTime : bytesToLoadByTime + (chunkSizeInBytes - (bytesToLoadByTime % chunkSizeInBytes)));
		}
		int divideInTimes = (int)Math.ceil((double)file.length() / (double)bytesToLoadByTime);
		
		String strongAlgorithmTemp = "-1";
		String weakAlgorithmTemp = "-1";
		
		for(int i = 0; i < divideInTimes; i++) {
			log.info("Searching in part " + i + "...");
			localIndex = 0;
			int moreBytesToLoad = 0;
			
			if(i == (divideInTimes - 1)) {
				bytesToLoadByTime = (int)(file.length() - globalIndex);				
			}
			
			log.debug("Memory: total[" + Runtime.getRuntime().totalMemory() + "] free[" + Runtime.getRuntime().freeMemory() + "]" +
					"used[" +  (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + "]");
									
			byte[] modFile = fileUtils.getBytesFromFile(file.getAbsolutePath(), offset, bytesToLoadByTime);
			byte[] currentChunk = new byte[chunkSizeInBytes];
			
			currentChunk = Arrays.copyOfRange(modFile, localIndex, 
					(localIndex + chunkSizeInBytes < modFile.length ? localIndex + chunkSizeInBytes : modFile.length));	
			c32.check(currentChunk, 0, currentChunk.length);
			while(localIndex < modFile.length) {								
				if(globalIndex % 1000 == 0) {
					log.debug("Global index: " + globalIndex);
				}
				
				if(modFile.length - localIndex < chunkSizeInBytes) {
					if(modFile.length - localIndex + moreBytesToLoad == chunkSizeInBytes) { //Sets up the last chunk with the default size to do not create new chunks unnecessarily, because with the default size it has a chance of to deduplicate						
						if(offset + (bytesToLoadByTime + moreBytesToLoad) > file.length()) { //Not to exceed the file size
							modFile = fileUtils.getBytesFromFile(file.getAbsolutePath(), offset, (int)(file.length() - offset));
							currentChunk = Arrays.copyOfRange(modFile, localIndex, modFile.length);
						} else {
							modFile = fileUtils.getBytesFromFile(file.getAbsolutePath(), offset, (bytesToLoadByTime + moreBytesToLoad));
							currentChunk = Arrays.copyOfRange(modFile, localIndex, localIndex + chunkSizeInBytes);
						}						
						c32.check(currentChunk, 0, currentChunk.length);
						offset += moreBytesToLoad;
					}
				}
				
				boolean differentChunk = true;
				if(chunksInfo.containsKey(c32.getValue())) {						
					if(currentChunk == null) { //To avoid reload the chunk
						currentChunk = Arrays.copyOfRange(modFile, localIndex, 
								(localIndex + chunkSizeInBytes < modFile.length ? localIndex + chunkSizeInBytes : modFile.length));
					}
					
					String strongHash = getStrongHash(hashingAlgorithm, currentChunk);
					if(chunksInfo.get(c32.getValue()).containsKey(strongHash)) {						
						if(buffer.position() > 0) { //If the buffer has some data, creates a chunk with this data								
							newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
							log.debug("[0] Creating new chunk " + chunk_number + " in " + (globalIndex - newchunk.length) + " [length = " + newchunk.length + "]");
							
							if(calculateAllHashes) {
								Checksum32 c32_2 = new Checksum32();
								c32_2.check(newchunk, 0, newchunk.length);
								weakAlgorithmTemp = String.valueOf(c32_2.getValue());
								strongAlgorithmTemp = getStrongHash(hashingAlgorithm, newchunk);
							}
							
							Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
									String.valueOf(globalIndex - newchunk.length), String.valueOf(newchunk.length));
							chunk.setStrongHash(strongAlgorithmTemp);
							chunk.setWeakHash(String.valueOf(weakAlgorithmTemp));
							chunk.setContent(newchunk.clone());
							
							newFileChunks.put(globalIndex - newchunk.length, chunk);
							
							chunk_number++;
							moreBytesToLoad += newchunk.length;
							buffer.clear();
						}						
						log.debug("Duplicated chunk " + chunk_number + ": " + strongHash + " [length = " + currentChunk.length + "]" + " [globalIndex = " + globalIndex + "]");
						
						Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
								String.valueOf(globalIndex), String.valueOf(currentChunk.length));
						chunk.setPfile(chunksInfo.get(c32.getValue()).get(strongHash).fileID);
						chunk.setPchunk(chunksInfo.get(c32.getValue()).get(strongHash).chunkID);
						
						newFileChunks.put(globalIndex, chunk);
						 
						chunk_number++;
						globalIndex += currentChunk.length;
						localIndex += currentChunk.length;	
						
						currentChunk = Arrays.copyOfRange(modFile, localIndex, 
								(localIndex + chunkSizeInBytes < modFile.length ? localIndex + chunkSizeInBytes : modFile.length));
						c32.check(currentChunk, 0, currentChunk.length);						
						differentChunk = false;
						referencesCount++;
					}					
				} 
				
				if(differentChunk) {
					currentChunk = null;
					if(buffer.remaining() == 0) {
						log.debug("[1] Creating new chunk " + chunk_number + " in " + (globalIndex - buffer.position()) + " [length = " + buffer.array().length + "]");
						Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
								String.valueOf(globalIndex - buffer.position()), String.valueOf(buffer.array().length));
						chunk.setStrongHash(getStrongHash(hashingAlgorithm, buffer.array()));
						chunk.setWeakHash(String.valueOf(c32.getValue()));
						chunk.setContent(buffer.array().clone());
						
						newFileChunks.put(globalIndex - buffer.position(), chunk);
						chunk_number++;
						buffer.clear();
					} else {
						if(modFile.length - (localIndex + chunkSizeInBytes) > 0) {
							buffer.put(modFile[localIndex]);
							c32.roll(modFile[localIndex + chunkSizeInBytes]);
							globalIndex++;
							localIndex++;
						} else {
							newchunk = Arrays.copyOfRange(modFile, localIndex - buffer.position(), (localIndex - buffer.position()) + 
								(modFile.length - (localIndex - buffer.position()) >= chunkSizeInBytes ? chunkSizeInBytes : modFile.length - (localIndex - buffer.position())));
							
							log.debug("[2] Creating new chunk " + chunk_number + " in " + (globalIndex - buffer.position()) + " [length = " + newchunk.length + "]");							
							if(calculateAllHashes) {
								c32.check(newchunk, 0, newchunk.length);
								weakAlgorithmTemp = String.valueOf(c32.getValue());
								strongAlgorithmTemp = getStrongHash(hashingAlgorithm, Arrays.copyOfRange(newchunk, 0, newchunk.length));
							}
							
							Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
									String.valueOf(globalIndex - buffer.position()), String.valueOf(newchunk.length));
							chunk.setStrongHash(strongAlgorithmTemp);
							chunk.setWeakHash(weakAlgorithmTemp);
							chunk.setContent(Arrays.copyOfRange(newchunk, 0, newchunk.length));
							
							newFileChunks.put(globalIndex - buffer.position(), chunk);							
							chunk_number++;
															
							localIndex += newchunk.length - buffer.position();
							globalIndex += newchunk.length - buffer.position();	
							
							buffer.clear();
							
							//Creating the final chunk, if has rest
							if(localIndex < modFile.length) {
								newchunk = Arrays.copyOfRange(modFile, localIndex, modFile.length);
								log.debug("[3] Creating new chunk " + chunk_number + " in " + (globalIndex) + " [length = " + newchunk.length + "]");
								
								if(calculateAllHashes) {
									c32.check(newchunk, 0, newchunk.length);
									weakAlgorithmTemp = String.valueOf(c32.getValue());
									strongAlgorithmTemp = getStrongHash(hashingAlgorithm, Arrays.copyOfRange(newchunk, 0, newchunk.length));
								}
								
								chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
										String.valueOf(globalIndex), String.valueOf(newchunk.length));
								chunk.setStrongHash(strongAlgorithmTemp);
								chunk.setWeakHash(weakAlgorithmTemp);
								chunk.setContent(Arrays.copyOfRange(newchunk, 0, newchunk.length));
								
								newFileChunks.put(globalIndex, chunk);
								
								chunk_number++;
								buffer.clear();
								
								localIndex += newchunk.length;
								globalIndex += newchunk.length;
							}
						}
					}			
				}								
			}
						
			//If the buffer has some data, creates a chunk with this data
			if(buffer.position() > 0) { 				
				log.debug("[4] Creating new chunk " + chunk_number + " in " + (globalIndex - buffer.position()) + " [length = " + buffer.array().length + "]");
				
				if(calculateAllHashes) {
					c32.check(buffer.array(), 0, buffer.capacity());
					weakAlgorithmTemp = String.valueOf(c32.getValue());
					strongAlgorithmTemp = getStrongHash(hashingAlgorithm, Arrays.copyOfRange(buffer.array(), 0, buffer.position()));
				}
				
				Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
						String.valueOf(globalIndex - buffer.position()), String.valueOf(buffer.capacity()));
				chunk.setStrongHash(strongAlgorithmTemp);
				chunk.setWeakHash(weakAlgorithmTemp);
				chunk.setContent(Arrays.copyOfRange(buffer.array(), 0, buffer.position()));
				newFileChunks.put(globalIndex - buffer.position(), chunk);
				
				chunk_number++;
				globalIndex += newchunk.length;
				buffer.clear();
			}
			
			for(Map.Entry<Long, Chunk> entry: newFileChunks.entrySet()) {				
				resultChunks.put(entry.getKey(), entry.getValue());			
			}
					
			newFileChunks.clear();
			FileUtils.cleanUpChunks(new String(System.getProperty("user.home") + System.getProperty("file.separator") +
					"chunks" + System.getProperty("file.separator")), file.getName());			
			
			offset += bytesToLoadByTime;			
		}
						
		log.info("Deduplicated in " + (System.currentTimeMillis() - time) + " miliseconds");
		return resultChunks;
	}
	
	/**
	 * Calculates the hashing function
	 * @param hashAlgorithm The hashing algorithm to use
	 * @param content Set of bytes to apply the hashing function
	 * @return hash value of the bytes set
	 * @throws HashingAlgorithmNotFound The algorithm specified was not found
	 */
	public static String getStrongHash(HashingAlgorithm hashAlgorithm, byte[] content) {
		switch(hashAlgorithm) {
			case MD5:
				return DigestUtils.md5Hex(content);			
			case SHA256:
				return DigestUtils.sha256Hex(content);
			case SHA384:
				return DigestUtils.sha384Hex(content);
			case SHA512:
				return DigestUtils.sha512Hex(content);
			case SHA1:
			default:
				return DigestUtils.sha1Hex(content);
		}
	}

}
