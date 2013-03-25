package com.dedupeer.backup;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Vector;

import javax.swing.JButton;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import com.dedupeer.chunking.Chunking;
import com.dedupeer.dao.operation.ChunksDaoOperations;
import com.dedupeer.dao.operation.FilesDaoOpeartion;
import com.dedupeer.dao.operation.UserFilesDaoOperations;
import com.dedupeer.exception.FieldNotFoundException;
import com.dedupeer.gui.component.renderer.ProgressInfo;
import com.dedupeer.processing.EagleEye;
import com.dedupeer.thrift.Chunk;
import com.dedupeer.thrift.ThriftClient;
import com.dedupeer.utils.FileUtils;
import com.deudpeer.checksum.Checksum32;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class StoredFile extends Observable implements StoredFileFeedback {
	
	public static final int FILE_NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	private static final int CHUNKS_TO_LOAD = 20;
	
	private static final Logger log = Logger.getLogger(StoredFile.class);
	private FileUtils fileUtils = new FileUtils();
	public int defaultChunkSize = Integer.parseInt(FileUtils.getPropertiesLoader().getProperties().getProperty("default.chunk.size"));
		
	private File file;
	private ProgressInfo progressInfo = new ProgressInfo(0, ProgressInfo.TYPE_CHUNKING);
	private String storageEconomy;
	private JButton btRestore;
	private String filename;	
	private byte[] newchunk;	
	private String pathToRestore;
	private int smallestChunk = -1;
	private long id = -1;
	
	/** Indicates if the hashes of all chunks must be calculated or if only hashes of chunks with default size.
	 * Drawback if false: do not deduplicate whole identical file because do not compares all chunks */
	private boolean calculateAllHashes = fileUtils.getPropertiesLoader().getProperties().getProperty("calculate.all.hashes").equalsIgnoreCase("true");
		
	public StoredFile(File file, String storageEconomy, long id) {
		this(file, storageEconomy);
		this.id = id;
	}
	
	public StoredFile(File file, String storageEconomy) {
		this(file, file.getName(), storageEconomy);
	}
	
	/** Store a file in the system with another name */
	public StoredFile(File file, String newFilename, String storageEconomy) {
		this.file = file;
		this.filename = newFilename;
		this.storageEconomy = storageEconomy;
	}
	
	public StoredFile(String filename, String storageEconomy, long id) {
		this(filename, storageEconomy);
		this.id = id;
	}
	
	public StoredFile(String filename, String storageEconomy) {
		this.filename = filename;
		this.storageEconomy = storageEconomy;
	}
	
	public File getFile() {
		return file;
	}
	
	public String getFilename() {
		return filename;
	}

	public int getProgress() {
		return progressInfo.getProgress();
	}

	public ProgressInfo getProgressInfo() {
		return progressInfo;
	}
	
	public String getStorageEconomy() {
		return storageEconomy;
	}

	public JButton getBtRestore() {
		return btRestore;
	}
	
	/**
	 * Get the values that are showed in the table
	 * @param item Item to get a value
	 * @return Item value
	 * @throws FieldNotFoundException The specified item do not exists
	 */
	public Object getItem(int item) throws FieldNotFoundException {
		switch (item) {
			case FILE_NAME:
				return getFilename();
			case PROGRESS:
				return progressInfo.getProgress();
			case ECONOMY:
				return storageEconomy;
		}
		throw new FieldNotFoundException();
	}
	
	/** Start a distribute file process between the peers in the network */
	public void store() {
		Thread storageProcess = new Thread(new Runnable() {
			
			@Override
			public void run() {
				long time = System.currentTimeMillis();
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
				
				if(!ufdo.fileExists(System.getProperty("username"), file.getName())) { //File is not in the system yet
					String fileID = String.valueOf(System.currentTimeMillis());
					ArrayList<Chunk> chunks = new ArrayList<Chunk>();
					try {						
						chunks = Chunking.slicingAndDicing(file, new String(System.getProperty("defaultPartition") + ":\\chunks\\"), defaultChunkSize, fileID, StoredFile.this); 
										
						progressInfo.setType(ProgressInfo.TYPE_STORING);
						ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", StoredFile.this);					
						cdo.insertRows(chunks);
						
						ufdo.insertRow(System.getProperty("username"), file.getName(), fileID, String.valueOf(file.length()), String.valueOf(chunks.size()), "?");
						fdo.insertRow(System.getProperty("username"), file.getName(), fileID);
						
						UserFilesDaoOperations udo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
						udo.setAmountChunksWithContent(System.getProperty("username"), file.getName(), chunks.size());
						udo.setAmountChunksWithoutContent(System.getProperty("username"), file.getName(), 0l);
						
						setId(Long.parseLong(fileID));
					} catch (IOException e) { 
						e.printStackTrace(); 
					}
				} else { //Other file version being stored
					deduplicate(file.getName()); //the same name
				}
								
				log.info("Stored in " + (System.currentTimeMillis() - time) + " miliseconds");				
				FileUtils.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());				
			}
		});		
		storageProcess.start();		
	}
	
	/**
	 * Deduplicates this file with a file passed as parameter
	 * @param filenameStored File name stored in the system to use to deduplicate this file
	 */
	public void deduplicate(String filenameStored) {		
		log.info("\n[Deduplicating...]");
		long time = System.currentTimeMillis();
		progressInfo.setType(ProgressInfo.TYPE_DEDUPLICATION);
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		
		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), filenameStored);				
		HColumn<String, String> columnAmountChunks = result.get().getSubColumnByName("chunks");		
		int amountChunk = Integer.parseInt(columnAmountChunks.getValue());
				
		String fileIDStored = ufdo.getValues(System.getProperty("username"), filenameStored).get().getSubColumnByName("file_id").getValue();		
		String newFileID = String.valueOf(System.currentTimeMillis());
		
		byte[] modFile = FileUtils.getBytesFromFile(file.getAbsolutePath());
		HashMap<Integer, Chunk> newFileChunks = new HashMap<Integer, Chunk>();
		int chunk_number = 0;
		
		Checksum32 c32 = new Checksum32();
		int lastIndex = 0;
		int lastLength = 0;
		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", this);
		
		long referencesCount = 0;
		//Find the duplicated chunk in the system
		for(int i = 0; i < amountChunk; i++) {
			HColumn<String, String> columnAdler32 = cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("adler32");
			HColumn<String, String> columnLength = cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("length");
			
			HColumn<String, String> columnMd5 = cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("md5");				
			
			int index = EagleEye.searchDuplication(modFile, Integer.parseInt(columnAdler32.getValue()), lastIndex + lastLength, Integer.parseInt(columnLength.getValue()));
			if(index != -1) {
				if(DigestUtils.md5Hex(Arrays.copyOfRange(modFile, index, index + Integer.parseInt(columnLength.getValue())))
						.equals(columnMd5.getValue())) {
					
					Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number++), String.valueOf(index), columnLength.getValue());
					chunk.setPfile(fileIDStored);
					chunk.setPchunk(String.valueOf(i));
					
					newFileChunks.put(index, chunk);
					lastIndex = index;
					lastLength = Integer.parseInt(columnLength.getValue());
					referencesCount++;
				}
			}
			setProgress((int)(((long)i * 100) / amountChunk));
		}
		
		int index = 0;
		ByteBuffer buffer = ByteBuffer.allocate(defaultChunkSize);
		while(index < modFile.length) {
			if(newFileChunks.containsKey(index)) {
				if(buffer.position() > 0) { //If the buffer have some data, creates a chunk with the data
					newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
					c32.check(newchunk, 0, newchunk.length);
					log.debug("Creating new chunk in " + (index - newchunk.length) + " [length = " + newchunk.length + "]");
					
					Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
							String.valueOf(index - buffer.position()), String.valueOf(newchunk.length));
					chunk.setMd5(DigestUtils.md5Hex(newchunk));
					chunk.setAdler32(String.valueOf(c32.getValue()));
					chunk.setContent(newchunk);
					
					newFileChunks.put(index - buffer.position(), chunk);
					chunk_number++;
					buffer.clear();
				}
				index += Long.parseLong(newFileChunks.get(index).length); //Skips, because the chunk was inserted in the comparison with other file
			} else {
				if(buffer.remaining() == 0) {
					c32.check(buffer.array(), 0, buffer.capacity());
					newchunk = buffer.array();
					log.debug("Creating new chunk in " + (index - newchunk.length) + " [length = " + newchunk.length + "]");
					
					Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
							String.valueOf(index - buffer.capacity()), String.valueOf(buffer.array().length));
					chunk.setAdler32(String.valueOf(c32.getValue()));
					chunk.setMd5(DigestUtils.md5Hex(newchunk));
					chunk.setContent(newchunk);
					
					newFileChunks.put(index - buffer.capacity(), chunk);
					chunk_number++;					
					buffer.clear();
				} else {
					buffer.put(modFile[index]);
					index++;
				}
			}
		}
		
		if(buffer.position() > 0) { //If the buffer have some data, creates a chunk with the data
			newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
			log.info("Creating new chunk in " + (index - newchunk.length) + " [length = " + newchunk.length + "]");
			
			Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
					String.valueOf(index - buffer.position()), String.valueOf(buffer.capacity()));
			chunk.setAdler32(String.valueOf(c32.getValue()));
			chunk.setMd5(DigestUtils.md5Hex(newchunk));
			chunk.setContent(newchunk);
			
			newFileChunks.put(index - buffer.position(), chunk);			
			buffer.clear();
		}
		log.info("Created chunks");
		
		int count = 0;
		progressInfo.setType(ProgressInfo.TYPE_STORING);
		for(Chunk chunk: newFileChunks.values()) {
			cdo.insertRow(chunk);
			setProgress((int)(Math.ceil((((double)count) * 100) / newFileChunks.size())));
		}
		
		ufdo.insertRow(System.getProperty("username"), getFilename(), newFileID, String.valueOf(file.length()), String.valueOf(chunk_number + 1), "?"); //+1 because start in 0
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getFilename(), newFileChunks.size() - referencesCount);
		ufdo.setAmountChunksWithoutContent(System.getProperty("username"), getFilename(), referencesCount);
		
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);		
		log.info("Deduplicated in " + (System.currentTimeMillis() - time) + " milisecods");		
		FileUtils.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());
	}
	
	/**
	 * Uses a new way of to deduplicate a file, comparing a current adler32 with a special HashMap that contains
	 * the adler32 in the key and other HashMap with <md5, chunkkNumber> how value
	 * divideInTimes divide the processing in <code> divideInTimes </code> times
	 * With this method, the deduplication is executed without Thrift.
	 * @param filenameStored
	 */
	public void deduplicateABigFile(String filenameStored, int bytesToLoadByTime) {		
		long time = System.currentTimeMillis();
		log.info("\n[Deduplicating...]");
		progressInfo.setType(ProgressInfo.TYPE_DEDUPLICATION);
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");

		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), filenameStored);				
		HColumn<String, String> columnAmountChunks = result.get().getSubColumnByName("chunks");		
		int amountChunks = Integer.parseInt(columnAmountChunks.getValue());

		String fileIDStored = ufdo.getValues(System.getProperty("username"), filenameStored).get().getSubColumnByName("file_id").getValue();		

		long timeToRetrieve = System.currentTimeMillis();

		//----------  Retrieving the information about the stored file -----------------		
		/** Map<adler32, Map<md5, chunkNumber>> */		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", this);				
		log.info("Retrieving chunks information...");
		Map<Integer, Map<String, String>> chunksInStorageServer = cdo.getHashesOfAFile(fileIDStored, amountChunks);		
		//--------------------------------------------------------------------------------
						
		log.info("Time to retrieve chunks information: " + (System.currentTimeMillis() - timeToRetrieve));
		String newFileID = String.valueOf(System.currentTimeMillis());
		HashMap<Long, Chunk> newFileChunks = new HashMap<Long, Chunk>();
		int chunk_number = 0;
		Checksum32 c32 = new Checksum32();
		
		long offset = 0;
		ByteBuffer buffer = ByteBuffer.allocate(defaultChunkSize);
		/** Current index in the divided byte array */
		int localIndex = 0;
		/** Index in the whole file */
		long globalIndex = 0;
		long referencesCount = 0;
		
		if(bytesToLoadByTime > file.length()) {
			bytesToLoadByTime = (int)file.length();
		} else {
			bytesToLoadByTime = (bytesToLoadByTime % defaultChunkSize == 0 ? bytesToLoadByTime : bytesToLoadByTime + (defaultChunkSize - (bytesToLoadByTime % defaultChunkSize)));
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
			byte[] currentChunk = new byte[defaultChunkSize];
			
			currentChunk = Arrays.copyOfRange(modFile, localIndex, 
					(localIndex + defaultChunkSize < modFile.length ? localIndex + defaultChunkSize : modFile.length));	
			c32.check(currentChunk, 0, currentChunk.length);
			while(localIndex < modFile.length) {								
				if(globalIndex % 1000 == 0) {
					log.debug("Global index: " + globalIndex);
				}
				
				if(modFile.length - localIndex < defaultChunkSize) {
					if(modFile.length - localIndex + moreBytesToLoad == defaultChunkSize) { //Sets up the last chunk with the default size to do not create new chunks unnecessarily, because with the default size it has a chance of to deduplicate						
						if(offset + (bytesToLoadByTime + moreBytesToLoad) > file.length()) { //Not to exceed the file size
							modFile = fileUtils.getBytesFromFile(file.getAbsolutePath(), offset, (int)(file.length() - offset));
							currentChunk = Arrays.copyOfRange(modFile, localIndex, modFile.length);
						} else {
							modFile = fileUtils.getBytesFromFile(file.getAbsolutePath(), offset, (bytesToLoadByTime + moreBytesToLoad));
							currentChunk = Arrays.copyOfRange(modFile, localIndex, localIndex + defaultChunkSize);
						}						
						c32.check(currentChunk, 0, currentChunk.length);
						offset += moreBytesToLoad;
					}
				}
				
				boolean differentChunk = true;
				if(chunksInStorageServer.containsKey(c32.getValue())) {						
					if(currentChunk == null) { //To avoid reload the chunk
						currentChunk = Arrays.copyOfRange(modFile, localIndex, 
								(localIndex + defaultChunkSize < modFile.length ? localIndex + defaultChunkSize : modFile.length));
					}
					
					String MD5 = DigestUtils.md5Hex(currentChunk);
					if(chunksInStorageServer.get(c32.getValue()).containsKey(MD5)) {						
						if(buffer.position() > 0) { //If the buffer has some data, creates a chunk with this data								
							newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
							log.debug("[0] Creating new chunk " + chunk_number + " in " + (globalIndex - newchunk.length) + " [length = " + newchunk.length + "]");
							
							if(calculateAllHashes) {
								Checksum32 c32_2 = new Checksum32();
								c32_2.check(newchunk, 0, newchunk.length);
								hash32Temp = String.valueOf(c32_2.getValue());
								MD5Temp = DigestUtils.md5Hex(newchunk);
							}
							
							Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
									String.valueOf(globalIndex - newchunk.length), String.valueOf(newchunk.length));
							chunk.setAdler32(String.valueOf(hash32Temp));
							chunk.setMd5(MD5Temp);
							chunk.setContent(newchunk);
							
							newFileChunks.put(globalIndex - newchunk.length, chunk);
							chunk_number++;
							moreBytesToLoad += newchunk.length;
							buffer.clear();
						}						
						log.debug("Duplicated chunk " + chunk_number + ": " + MD5 + " [length = " + currentChunk.length + "]" + " [globalIndex = " + globalIndex + "]");
						
						Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
								String.valueOf(globalIndex), String.valueOf(currentChunk.length));
						chunk.setPfile(fileIDStored);
						chunk.setPchunk(chunksInStorageServer.get(c32.getValue()).get(MD5));
						
						newFileChunks.put(globalIndex, chunk);						
						chunk_number++;
						globalIndex += currentChunk.length;
						localIndex += currentChunk.length;	
						
						currentChunk = Arrays.copyOfRange(modFile, localIndex, 
								(localIndex + defaultChunkSize < modFile.length ? localIndex + defaultChunkSize : modFile.length));
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
						chunk.setAdler32(String.valueOf(c32.getValue()));
						chunk.setMd5(DigestUtils.md5Hex(buffer.array()));
						chunk.setContent(buffer.array());
						
						newFileChunks.put(globalIndex - buffer.position(), chunk);
						chunk_number++;
						buffer.clear();
					} else {
						if(modFile.length - (localIndex + defaultChunkSize) > 0) {
							buffer.put(modFile[localIndex]);
							c32.roll(modFile[localIndex + defaultChunkSize]);
							globalIndex++;
							localIndex++;
						} else {
							newchunk = Arrays.copyOfRange(modFile, localIndex - buffer.position(), (localIndex - buffer.position()) + 
								(modFile.length - (localIndex - buffer.position()) >= defaultChunkSize ? defaultChunkSize : modFile.length - (localIndex - buffer.position())));
							
							log.debug("[2] Creating new chunk " + chunk_number + " in " + (globalIndex - buffer.position()) + " [length = " + newchunk.length + "]");							
							if(calculateAllHashes) {
								c32.check(newchunk, 0, newchunk.length);
								hash32Temp = String.valueOf(c32.getValue());
								MD5Temp = DigestUtils.md5Hex(Arrays.copyOfRange(newchunk, 0, newchunk.length));
							}
							
							Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
									String.valueOf(globalIndex - buffer.position()), String.valueOf(newchunk.length));
							chunk.setAdler32(hash32Temp);
							chunk.setMd5(MD5Temp);
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
									hash32Temp = String.valueOf(c32.getValue());
									MD5Temp = DigestUtils.md5Hex(Arrays.copyOfRange(newchunk, 0, newchunk.length));
								}
								
								chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
										String.valueOf(globalIndex), String.valueOf(newchunk.length));
								chunk.setAdler32(hash32Temp);
								chunk.setMd5(MD5Temp);
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
					hash32Temp = String.valueOf(c32.getValue());
					MD5Temp = DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position()));
				}
				
				Chunk chunk = new Chunk(String.valueOf(newFileID), String.valueOf(chunk_number), 
						String.valueOf(globalIndex - buffer.position()), String.valueOf(buffer.capacity()));
				chunk.setAdler32(hash32Temp);
				chunk.setMd5(MD5Temp);
				chunk.setContent(Arrays.copyOfRange(buffer.array(), 0, buffer.position()));
				
				newFileChunks.put(globalIndex - buffer.position(), chunk);
				
				chunk_number++;
				globalIndex += newchunk.length;
				buffer.clear();
			}
			
			progressInfo.setType(ProgressInfo.TYPE_STORING);
			
			for(Chunk chunk: newFileChunks.values()) {				
				cdo.insertRow(chunk);
			}
			
			setProgress((int)(Math.ceil((((double)globalIndex) * 100) / file.length())));			
			newFileChunks.clear();			
			FileUtils.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());			
			
			offset += bytesToLoadByTime;
			
		}
		
		//Last time		
		ufdo.insertRow(System.getProperty("username"), getFilename(), newFileID, String.valueOf(file.length()), String.valueOf(chunk_number), "?"); //+1 because start in 0
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getFilename(), chunk_number - referencesCount);
		ufdo.setAmountChunksWithoutContent(System.getProperty("username"), getFilename(), referencesCount);
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);		
		
		log.info("Deduplicated in " + (System.currentTimeMillis() - time) + " miliseconds");		
	}
	
	/**
	 * Deduplicate a file using ThriftClient
	 * @param filenameStored Name of file stored in the system
	 * @param bytesToLoadByTime Amount of bytes to load in memory by time
	 * @param pathOfFile Path of file to analyze
	 */
	public void deduplicateABigFileByThrift(String filenameStored, int bytesToLoadByTime) {
		long time = System.currentTimeMillis();
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), filenameStored);				
		HColumn<String, String> columnAmountChunks = result.get().getSubColumnByName("chunks");		
		int amountChunks = Integer.parseInt(columnAmountChunks.getValue());

		String fileIDStored = ufdo.getValues(System.getProperty("username"), filenameStored).get().getSubColumnByName("file_id").getValue();		
		String newFileID = String.valueOf(System.currentTimeMillis());
		
		//----------  Retrieving the information about the stored file -----------------		
		/** Map<adler32, Map<md5, chunkNumber>> */		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", this);				
		log.info("Retrieving chunks information...");
		Map<Integer, Map<String, String>> chunksInStorageServer = cdo.getHashesOfAFile(fileIDStored, amountChunks);		
		//--------------------------------------------------------------------------------
		
		Map<Long,Chunk> newFileChunks = ThriftClient.getInstance().deduplicate(chunksInStorageServer, file.getAbsolutePath(), defaultChunkSize, bytesToLoadByTime);
		
		int totalChunks = 0;
		int referencesCount = 0;
		
		for(Chunk chunk: newFileChunks.values()) {				
			cdo.insertRow(chunk);
			totalChunks++;
			if(chunk.getPchunk() != null) {
				referencesCount++;
			}
		}
		
		ufdo.insertRow(System.getProperty("username"), getFilename(), newFileID, String.valueOf(file.length()), String.valueOf(totalChunks), "?");
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getFilename(), totalChunks - referencesCount);
		ufdo.setAmountChunksWithoutContent(System.getProperty("username"), getFilename(), referencesCount);
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);		
		
		log.info("Deduplicated in " + (System.currentTimeMillis() - time) + " miliseconds");	
	}
	
	/** Retrieves the file, even though it is deduplicated */
	public void rehydrate() {
		Thread restoreProcess = new Thread(new Runnable() {
			@Override
			public void run() {
				long time = System.currentTimeMillis();
				progressInfo.setType(ProgressInfo.TYPE_REHYDRATING);
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");				
				ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", StoredFile.this);
				
				long amountTotalChunks = ufdo.getChunksCount(System.getProperty("username"), getFilename());
				long amountChunksWithContent = ufdo.getChunksWithContentCount(System.getProperty("username"), getFilename());				
				long count = 0;
				long amountChunksWithContentLoaded = 0;
				
				progressInfo.setType(ProgressInfo.TYPE_WRITING);
				long initialChunkToLoad = 0;
				while(amountChunksWithContent - amountChunksWithContentLoaded > 0) {
					int amountChunksToLoad = (int)(amountChunksWithContent - amountChunksWithContentLoaded >= CHUNKS_TO_LOAD ? CHUNKS_TO_LOAD : amountChunksWithContent - amountChunksWithContentLoaded);
					Vector<QueryResult<HSuperColumn<String, String, String>>> chunksWithContent = cdo.getValuesWithContent(System.getProperty("username"), filename, initialChunkToLoad, amountChunksToLoad);					
					
					for(QueryResult<HSuperColumn<String, String, String>> chunk: chunksWithContent) {
						log.debug(chunk.get().getName() + "[index = " + chunk.get().getSubColumnByName("index").getValue() + 
								"] [length = " + BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()).length + "]");
						
						FileUtils.storeFileLocally(BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()), Long.parseLong(chunk.get().getSubColumnByName("index").getValue())
								, pathToRestore + "\\" + filename);						
						count++;					
						int prog = (int)(Math.ceil((((double)count) * 100) / amountTotalChunks));
						setProgress(prog);						
						initialChunkToLoad = Long.parseLong(chunk.get().getName());
					}
					
					if(chunksWithContent.size() > 0) {
						initialChunkToLoad++;
					}
					
					amountChunksWithContentLoaded += chunksWithContent.size();
					chunksWithContent.clear();
				}
				
				long amountChunksWithoutContent = ufdo.getChunksWithoutContentCount(System.getProperty("username"), getFilename());
				
				count = 0;
				long amountChunksWithoutContentLoaded = 0;
				initialChunkToLoad = 0;
				while(amountChunksWithoutContent - amountChunksWithoutContentLoaded > 0) {
					int amountChunksToLoad = (int)(amountChunksWithoutContent - amountChunksWithoutContentLoaded  >= CHUNKS_TO_LOAD ? CHUNKS_TO_LOAD : amountChunksWithoutContent - amountChunksWithoutContentLoaded);
					Vector<QueryResult<HSuperColumn<String, String, String>>> chunksReference = cdo.getValuesWithoutContent(System.getProperty("username"), filename, initialChunkToLoad, amountChunksToLoad);
					
					for(QueryResult<HSuperColumn<String, String, String>> chunkReference: chunksReference) {
						//Retrieves by reference
						QueryResult<HSuperColumn<String, String, String>> chunk = cdo.getValues(chunkReference.get().getSubColumnByName("pfile").getValue(), 
								chunkReference.get().getSubColumnByName("pchunk").getValue());
					
						log.info(chunkReference.get().getName() + " [index = " + chunkReference.get().getSubColumnByName("index").getValue() + 
								"] [length = " + BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()).length + "]");
						FileUtils.storeFileLocally(BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()), Long.parseLong(chunkReference.get().getSubColumnByName("index").getValue())
								, pathToRestore + "\\" + filename);	
						
						count++;
						int prog = (int)(Math.ceil((((double)count) * 100) / amountTotalChunks));
						setProgress(prog);						
						initialChunkToLoad = Long.parseLong(chunkReference.get().getName());
					}
					
					if(chunksReference.size() > 0) 
						initialChunkToLoad++;
					
					amountChunksWithoutContentLoaded += chunksReference.size();
					chunksReference.clear();
				}
				
				setProgress(0);
				log.info("Rehydrated in " + (System.currentTimeMillis() - time) + " miliseconds");
				
			}
		});
		restoreProcess.start();
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}
	
	public void setPathToRestore(String path) {
		this.pathToRestore = path;
	}

	public void setProgress(int progress) {		
		this.progressInfo.setProgress(progress);
		valueChanged();
	}
	
	public void setStorageEconomy(String storageEconomy) {
		this.storageEconomy = storageEconomy;
		valueChanged();
	}

	public void setFilename(String filename) {
		this.filename = filename;		 
		valueChanged();
	}

	private void valueChanged() {
		setChanged();
		notifyObservers();
	}
	
	public void calculateStorageEconomy() {
		Thread calculateProcess = new Thread(new Runnable() {
			@Override
			public void run() {				
				log.info("Calculating storage economy of " + getFilename() + "...");
				
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				long fileLength = ufdo.getFileLength(System.getProperty("username"), getFilename());
				long amountOfChunksWithContent = ufdo.getChunksWithContentCount(System.getProperty("username"), getFilename());
				long totalChunks = ufdo.getChunksCount(System.getProperty("username"), getFilename());
				
				if(totalChunks == amountOfChunksWithContent) {					
					setStorageEconomy("0%");
					progressInfo.setProgress(100);
				} else {				
					ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", StoredFile.this);
					progressInfo.setType(ProgressInfo.TYPE_CALCULATION_STORAGE_ECONOMY);					
					long bytesStored = cdo.getSpaceOccupiedByTheFile(System.getProperty("username"), getFilename());									
					setStorageEconomy((100 - ((bytesStored * 100) / fileLength)) + "%");
					log.info("Storage economy of " + getFilename() + " = " + getStorageEconomy());					
					progressInfo.setProgress(100);
				}
			}
		});
		calculateProcess.start();
	
	}
	
	@Override
	public void updateProgress(int progress) {
		setProgress(progress);		
	}

	@Override
	public void setProgressType(int type) {
		progressInfo.setType(type);
	}

	public int getSmallestChunk() {
		return smallestChunk;
	}
	
}
