package com.dedupeer.thrift;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.dedupeer.checksum.rsync.Checksum32;
import com.dedupeer.dao.ChunksDao;
import com.dedupeer.dao.operation.FilesDaoOpeartion;
import com.dedupeer.dao.operation.UserFilesDaoOperations;
import com.dedupeer.gui.component.renderer.ProgressInfo;
import com.dedupeer.processing.file.Chunking;
import com.dedupeer.utils.FileUtils;

public class DeduplicationServiceImpl implements DeduplicationService.Iface {

	private static final Logger log = Logger.getLogger(DeduplicationServiceImpl.class);
	private FileUtils fileUtils = new FileUtils();
	
	@Override
	public Map<Long, Chunk> deduplicate(
			Map<Integer, Map<String, String>> chunksInfo, String pathOfFile,
			int chunkSizeInBytes, int bytesToLoadByTime) throws TException {
		long time = System.currentTimeMillis();
		log.info("\n[Deduplicating...]");
		
		long timeToRetrieve = System.currentTimeMillis();
				
		String newFileID = String.valueOf(System.currentTimeMillis());
		HashMap<Long, ChunksDao> newFileChunks = new HashMap<Long, ChunksDao>();
		int chunk_number = 0;
		Checksum32 c32 = new Checksum32();
		
		long offset = 0;
		
		//TODO mudar parâmetro para int
		ByteBuffer buffer = ByteBuffer.allocate((int)chunkSizeInBytes);
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
		
		String MD5Temp = "-1";
		String hash32Temp = "-1";
		
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
					
					String MD5 = DigestUtils.md5Hex(currentChunk);
					if(chunksInfo.get(c32.getValue()).containsKey(MD5)) {						
						if(buffer.position() > 0) { //If the buffer has some data, creates a chunk with this data								
							newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
							log.debug("[0] Creating new chunk " + chunk_number + " in " + (globalIndex - newchunk.length) + " [length = " + newchunk.length + "]");
							
							if(calculateAllHashes) {
								Checksum32 c32_2 = new Checksum32();
								c32_2.check(newchunk, 0, newchunk.length);
								hash32Temp = String.valueOf(c32_2.getValue());
								MD5Temp = DigestUtils.md5Hex(newchunk);
							}
							
							newFileChunks.put(globalIndex - newchunk.length, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
									MD5Temp, String.valueOf(hash32Temp), String.valueOf(globalIndex - newchunk.length), 
									String.valueOf(newchunk.length), newchunk));
							chunk_number++;
							moreBytesToLoad += newchunk.length;
							buffer.clear();
						}						
						log.debug("Duplicated chunk " + chunk_number + ": " + MD5 + " [length = " + currentChunk.length + "]" + " [globalIndex = " + globalIndex + "]");
						
						newFileChunks.put(globalIndex, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
							String.valueOf(globalIndex), String.valueOf(currentChunk.length), fileIDStored, chunksInfo.get(c32.getValue()).get(MD5)));						
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
						newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
								DigestUtils.md5Hex(buffer.array()), String.valueOf(c32.getValue()), String.valueOf(globalIndex - buffer.position()), 
								String.valueOf(buffer.array().length), buffer.array()));
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
								hash32Temp = String.valueOf(c32.getValue());
								MD5Temp = DigestUtils.md5Hex(Arrays.copyOfRange(newchunk, 0, newchunk.length));
							}
							
							newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
									MD5Temp, hash32Temp, String.valueOf(globalIndex - buffer.position()), 
									String.valueOf(newchunk.length), Arrays.copyOfRange(newchunk, 0, newchunk.length)));							
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
									hash32Temp = String.valueOf(c32.getValue());
									MD5Temp = DigestUtils.md5Hex(Arrays.copyOfRange(newchunk, 0, newchunk.length));
								}
								
								newFileChunks.put(globalIndex, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
										MD5Temp, hash32Temp, String.valueOf(globalIndex), 
										String.valueOf(newchunk.length), Arrays.copyOfRange(newchunk, 0, newchunk.length)));
								
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
					hash32Temp = String.valueOf(c32.getValue());
					MD5Temp = DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position()));
				}
				
				newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
						MD5Temp, hash32Temp, String.valueOf(globalIndex - buffer.position()), 
						String.valueOf(buffer.capacity()), Arrays.copyOfRange(buffer.array(), 0, buffer.position())));
				
				chunk_number++;
				globalIndex += newchunk.length;
				buffer.clear();
			}
			
			progressInfo.setType(ProgressInfo.TYPE_STORING);
			
			for(ChunksDao chunk: newFileChunks.values()) {				
				cdo.insertRow(chunk);			
			}
			
			setProgress((int)(Math.ceil((((double)globalIndex) * 100) / file.length())));			
			newFileChunks.clear();			
			Chunking.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());			
			
			offset += bytesToLoadByTime;
			
		}
		
		//Last time		
		ufdo.insertRow(System.getProperty("username"), getFilename(), newFileID, String.valueOf(file.length()), String.valueOf(chunk_number), "?"); //+1 because start in 0
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getFilename(), chunk_number - referencesCount);
		ufdo.setAmountChunksWithoutContent(System.getProperty("username"), getFilename(), referencesCount);		
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);		
		log.info("Deduplicated in " + (System.currentTimeMillis() - time) + " miliseconds");
	
	}

}
