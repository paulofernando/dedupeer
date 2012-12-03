package deduplication.backup;

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

import deduplication.checksum.rsync.Checksum32;
import deduplication.dao.ChunksDao;
import deduplication.dao.operation.ChunksDaoOperations;
import deduplication.dao.operation.FilesDaoOpeartion;
import deduplication.dao.operation.UserFilesDaoOperations;
import deduplication.exception.FieldNotFoundException;
import deduplication.gui.component.renderer.ProgressInfo;
import deduplication.processing.EagleEye;
import deduplication.processing.file.Chunking;
import deduplication.utils.FileUtils;

public class StoredFile extends Observable implements StoredFileFeedback {
	
	public static final int defaultChunkSize = 128000;
	private static final Logger log = Logger.getLogger(StoredFile.class);
	
	public static final int FILE_NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	
	private File file;
	private ProgressInfo progressInfo = new ProgressInfo(0, ProgressInfo.TYPE_CHUNKING);
	private String storageEconomy;
	private JButton btRestore;
	private String filename;
	
	private byte[] newchunk;
	
	private int smallestChunk = -1;
	
	private String pathToRestore;
	
	private long id = -1;
	
	private static final int CHUNKS_TO_LOAD = 20;
		
	public StoredFile(File file, String storageEconomy, long id) {
		this(file, storageEconomy);
		this.id = id;
	}
	
	public StoredFile(File file, String storageEconomy) {
		this(file, file.getName(), storageEconomy);
	}
	
	/**
	 * Store a file in the system with another name
	 */
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
	
	/**
	 * Start a distribute file process between the peers in the network
	 */
	public void store() {
		Thread storageProcess = new Thread(new Runnable() {
			
			@Override
			public void run() {
				long time = System.currentTimeMillis();
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
				
				if(!ufdo.fileExists(System.getProperty("username"), file.getName())) { //File is not in the system yet
					String fileID = String.valueOf(System.currentTimeMillis());
					ArrayList<ChunksDao> chunks = new ArrayList<ChunksDao>();
					try {						
						chunks = Chunking.slicingAndDicing(file, new String(System.getProperty("defaultPartition") + ":\\chunks\\"), defaultChunkSize, fileID, StoredFile.this); 
					} catch (IOException e) { 
						e.printStackTrace(); 
					}
					
					ufdo.insertRow(System.getProperty("username"), file.getName(), fileID, String.valueOf(file.length()), String.valueOf(chunks.size()), "?");
					fdo.insertRow(System.getProperty("username"), file.getName(), fileID);
					
					progressInfo.setType(ProgressInfo.TYPE_STORING);
					ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", StoredFile.this);					
					cdo.insertRows(chunks);
					
					UserFilesDaoOperations udo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
					udo.setAmountChunksWithContent(System.getProperty("username"), file.getName(), chunks.size());
					udo.setAmountChunksWithoutContent(System.getProperty("username"), file.getName(), 0l);
					
					setId(Long.parseLong(fileID));
				} else { //Other file version being stored
					deduplicate(file.getName()); //the same name
				}
								
				log.info("Stored in " + (System.currentTimeMillis() - time) + " miliseconds");
				
				Chunking.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());
			}
		});
		
		storageProcess.start();		
	}
	
	/**
	 * Deduplicates this file with a file passed as parameter
	 * @param filenameStored File name stored in the system to use to deduplicate this file
	 */
	public void deduplicate(String filenameStored) {
		progressInfo.setType(ProgressInfo.TYPE_DEDUPLICATION);
		
		long time = System.currentTimeMillis();
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		
		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), filenameStored);				
		HColumn<String, String> columnAmountChunks = result.get().getSubColumnByName("chunks");		
		int amountChunk = Integer.parseInt(columnAmountChunks.getValue());
				
		String fileIDStored = ufdo.getValues(System.getProperty("username"), filenameStored).get().getSubColumnByName("file_id").getValue();		
		String newFileID = String.valueOf(System.currentTimeMillis());
		
		byte[] modFile = FileUtils.getBytesFromFile(file.getAbsolutePath());
		HashMap<Integer, ChunksDao> newFileChunks = new HashMap<Integer, ChunksDao>();
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
					newFileChunks.put(index, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number++), String.valueOf(index), columnLength.getValue(),
							fileIDStored, String.valueOf(i)));
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
				if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele
					newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
					c32.check(newchunk, 0, newchunk.length);
					newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
							DigestUtils.md5Hex(newchunk), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
							String.valueOf(newchunk.length), newchunk));
					chunk_number++;
					buffer.clear();
				}
				index += Long.parseLong(newFileChunks.get(index).length); //pula, porque o chunk já foi inserido na comparação com o outro arquivo
			} else {
				if(buffer.remaining() == 0) {
					c32.check(buffer.array(), 0, buffer.capacity());
					newFileChunks.put(index - buffer.capacity(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
							DigestUtils.md5Hex(buffer.array()), String.valueOf(c32.getValue()), String.valueOf(index - buffer.capacity()), 
							String.valueOf(buffer.array().length), buffer.array()));
					chunk_number++;					
					buffer.clear();
				} else {
					buffer.put(modFile[index]);
					index++;
				}
			}
		}
		if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele			
			newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
					DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position())), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
					String.valueOf(buffer.capacity()), Arrays.copyOfRange(buffer.array(), 0, buffer.position())));
			
			buffer.clear();
		}
		
		
		int count = 0;
		progressInfo.setType(ProgressInfo.TYPE_STORING);
		for(ChunksDao chunk: newFileChunks.values()) {
			cdo.insertRow(chunk);
			setProgress((int)(Math.ceil((((double)count) * 100) / newFileChunks.size())));
		}
		
		ufdo.insertRow(System.getProperty("username"), getFilename(), newFileID, String.valueOf(file.length()), String.valueOf(chunk_number + 1), "?"); //+1 because start in 0
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getFilename(), newFileChunks.size() - referencesCount);
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getFilename(), referencesCount);
		
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);
		
		log.info("Deduplicated in " + (System.currentTimeMillis() - time) + " milisecods");
		
		Chunking.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());
	}
	
	/**
	 * Uses a new way of to deduplicate a file, comparing a current adler32 with a special HashMap that contains
	 * the adler32 in the key and other HashMap with <md5, chunkkNumber> how value
	 * divideInTimes divide the processing in <code> divideInTimes </code> times
	 * @param filenameStored
	 */
	public void deduplicateABigFile(String filenameStored, int divideInTimes) {
		progressInfo.setType(ProgressInfo.TYPE_DEDUPLICATION);
		
		long time = System.currentTimeMillis();
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		
		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), filenameStored);				
		HColumn<String, String> columnAmountChunks = result.get().getSubColumnByName("chunks");		
		int amountChunk = Integer.parseInt(columnAmountChunks.getValue());
				
		String fileIDStored = ufdo.getValues(System.getProperty("username"), filenameStored).get().getSubColumnByName("file_id").getValue();		
		
		long timeToRetrieve = System.currentTimeMillis();
		//----------  Retrieving the information about the stored file -----------------		
		/** Map<adler32, Map<md5, chunkNumber>> */
		Map<Integer, Map<String, String>> fileInStorageServer = new HashMap<Integer, Map<String, String>>();		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", this);	
		log.info("Retrieving chunks information...");
		for(int i = 0; i < amountChunk; i++) {			
			String adler32 = cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("adler32").getValue();
			
			if(!fileInStorageServer.containsKey(adler32)) {
				Map<String, String> chunkInfo = new HashMap<String, String>();
				chunkInfo.put(cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("md5").getValue(),
						String.valueOf(i));
				fileInStorageServer.put(Integer.parseInt(adler32), chunkInfo);
			} else {
				Map<String, String> md5Set = fileInStorageServer.get(adler32);
				md5Set.put(cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("md5").getValue(),
						String.valueOf(i));
			}
			log.info("chunk " + i);
		}
				
		//--------------------------------------------------------------------------------
		System.out.println("Time to retrieve chunks information: " + (System.currentTimeMillis() - timeToRetrieve));
		String newFileID = String.valueOf(System.currentTimeMillis());
		HashMap<Long, ChunksDao> newFileChunks = new HashMap<Long, ChunksDao>();
		int chunk_number = 0;
		
		Checksum32 c32 = new Checksum32();
		int lastIndex = 0;
		int lastLength = 0;
		
		long offset = 0;
		ByteBuffer buffer = ByteBuffer.allocate(defaultChunkSize);
		/* current index in the divided byte array */
		int localIndex = 0;
		/* index in the whole file */
		long globalIndex = 0;	
		
		long referencesCount = 0;
		
		for(int i = 0; i < divideInTimes; i++) {
			log.info("Searching in part " + i + "...");
			localIndex = 0;
			int bytesToRead;
			if(i != (divideInTimes - 1)) {
				bytesToRead = (int) Math.ceil(((double)file.length() / (double)divideInTimes));
				bytesToRead = (bytesToRead % defaultChunkSize == 0 ? bytesToRead : bytesToRead + (defaultChunkSize - (bytesToRead % defaultChunkSize)));
			} else {
				bytesToRead = (int) Math.ceil(((double)file.length() / (double)divideInTimes));
				bytesToRead = (bytesToRead % defaultChunkSize == 0 ? bytesToRead : bytesToRead + (defaultChunkSize - (bytesToRead % defaultChunkSize)));
				if(file.length() % divideInTimes != 0) {
					bytesToRead = (int)(file.length() - (bytesToRead * (i)));
				}
			}
			
			System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());
			System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
			System.out.println("Usage memory:" +  (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
			
			byte[] modFile = FileUtils.getBytesFromFile(file.getAbsolutePath(), offset, bytesToRead);
			byte[] currentChunk = new byte[defaultChunkSize];
					
			currentChunk = Arrays.copyOfRange(modFile, localIndex, localIndex + defaultChunkSize);	
			c32.check(currentChunk, 0, currentChunk.length);
			while(localIndex < modFile.length) {
					if(fileInStorageServer.containsKey(c32.getValue())) {						
						currentChunk = Arrays.copyOfRange(modFile, localIndex, localIndex + defaultChunkSize);
						String MD5 = DigestUtils.md5Hex(currentChunk);
						if(fileInStorageServer.get(c32.getValue()).containsKey(MD5)) {
							if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele								
								newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
								log.info("Creating new chunk: " + (globalIndex - newchunk.length));
								Checksum32 c32_2 = new Checksum32();
								c32_2.check(newchunk, 0, newchunk.length);
								newFileChunks.put(globalIndex - newchunk.length, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
										DigestUtils.md5Hex(newchunk), String.valueOf(c32_2.getValue()), String.valueOf(globalIndex - buffer.position()), 
										String.valueOf(newchunk.length), newchunk));
								chunk_number++;
								buffer.clear();
							}							
							
							log.info("Duplicated chunk: " + MD5);
							
							newFileChunks.put(globalIndex, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
								String.valueOf(globalIndex), String.valueOf(currentChunk.length), fileIDStored, fileInStorageServer.get(c32.getValue()).get(MD5)));
							
							chunk_number++;
							globalIndex += currentChunk.length;
							localIndex += currentChunk.length;
							
							currentChunk = Arrays.copyOfRange(modFile, localIndex, localIndex + defaultChunkSize);
							c32.check(currentChunk, 0, currentChunk.length);
							
							referencesCount++;
						} else {
							if(buffer.remaining() == 0) { //same of the else below, but for do not calculate the MD5 always it was copied
								log.info("Creating new chunk " + (globalIndex - buffer.position()));
								newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
										DigestUtils.md5Hex(buffer.array()), String.valueOf(c32.getValue()), String.valueOf(globalIndex - buffer.position()), 
										String.valueOf(buffer.array().length), buffer.array()));
								chunk_number++;
								
								
								buffer.clear();
							} else {
								if(modFile.length - (localIndex + defaultChunkSize) > 0) {
									buffer.put(modFile[localIndex]);
									c32.roll(modFile[localIndex + defaultChunkSize]);									
									globalIndex++;
									localIndex++;									
								}
							}
						}
					} else {
						if(buffer.remaining() == 0) {
							log.info("Creating new chunk" + (globalIndex - buffer.position()));
							newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
									DigestUtils.md5Hex(buffer.array()), String.valueOf(c32.getValue()), String.valueOf(globalIndex - buffer.position()), 
									String.valueOf(buffer.array().length), buffer.array()));
							chunk_number++;
							buffer.clear();
						} else {
							if(modFile.length - (localIndex + defaultChunkSize) > 0) {
								buffer.put(modFile[localIndex]);
								c32.roll(modFile[localIndex + defaultChunkSize]);
								globalIndex++;
								localIndex++;
							} else {
								newchunk = Arrays.copyOfRange(modFile, localIndex, modFile.length); //não utiliza o byteBuffer porque o chunk pode ser menor que defaulChunkSize, daí o cálculo do hash fica errado
								log.info("Creating new chunk in " + (globalIndex));
								
								c32.check(newchunk, 0, newchunk.length);
								
								newFileChunks.put(globalIndex, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
										DigestUtils.md5Hex(Arrays.copyOfRange(newchunk, 0, newchunk.length)), String.valueOf(c32.getValue()), String.valueOf(globalIndex), 
										String.valueOf(newchunk.length), Arrays.copyOfRange(newchunk, 0, newchunk.length)));
								
								chunk_number++;
								buffer.clear();
								
								localIndex += newchunk.length;
								globalIndex += newchunk.length;	
							}
						}			
					}			
			}
						
			if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele				
				log.info("Creating new chunk" + (globalIndex - buffer.position()));
				
				//TODO Otimizar aqui para utilizar o roll da linha 373
				c32.check(buffer.array(), 0, buffer.capacity());
				
				newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
						DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position())), String.valueOf(c32.getValue()), String.valueOf(globalIndex - buffer.position()), 
						String.valueOf(buffer.capacity()), Arrays.copyOfRange(buffer.array(), 0, buffer.position())));
				
				chunk_number++;
				buffer.clear();
			}
			
			int count = 0;
			progressInfo.setType(ProgressInfo.TYPE_STORING);
			System.out.println("Storing...");
			for(ChunksDao chunk: newFileChunks.values()) {				
				cdo.insertRow(chunk);
				setProgress((int)(Math.ceil((((double)count) * 100) / newFileChunks.size())));
			}
			
			newFileChunks.clear();			
			Chunking.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());			
			
			offset += bytesToRead;
		}
		
		//last time
		
		ufdo.insertRow(System.getProperty("username"), getFilename(), newFileID, String.valueOf(file.length()), String.valueOf(chunk_number), "?"); //+1 because start in 0
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getFilename(), chunk_number - referencesCount);
		ufdo.setAmountChunksWithoutContent(System.getProperty("username"), getFilename(), referencesCount);
		
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);
		
		log.info("Deduplicated in " + (System.currentTimeMillis() - time) + " miliseconds");
		
	}
	
	/**
	 * Retrieves the file, even though it is deduplicated
	 */
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
				while(amountChunksWithContent - amountChunksWithContentLoaded > 0) {
					int amountChunksToLoad = (int)(amountChunksWithContent - amountChunksWithContentLoaded >= CHUNKS_TO_LOAD ? CHUNKS_TO_LOAD : amountChunksWithContent - amountChunksWithContentLoaded);
					Vector<QueryResult<HSuperColumn<String, String, String>>> chunksWithContent = cdo.getValuesWithContent(System.getProperty("username"), filename, amountChunksWithContentLoaded, amountChunksToLoad);
					
					for(QueryResult<HSuperColumn<String, String, String>> chunk: chunksWithContent) {
												
						FileUtils.storeFileLocally(BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()), Long.parseLong(chunk.get().getSubColumnByName("index").getValue())
								, pathToRestore + "\\" + filename);
						
						count++;					
						int prog = (int)(Math.ceil((((double)count) * 100) / amountTotalChunks));
						setProgress(prog);
					}
					amountChunksWithContentLoaded += chunksWithContent.size();
					chunksWithContent.clear();
				}
				
				long amountChunksWithoutContent = ufdo.getChunksWithoutContentCount(System.getProperty("username"), getFilename());
				
				count = 0;
				long amountChunksWithoutContentLoaded = 0;
				while(amountChunksWithoutContent - amountChunksWithoutContentLoaded > 0) {
					int amountChunksToLoad = (int)(amountChunksWithoutContent - amountChunksWithoutContentLoaded  >= CHUNKS_TO_LOAD ? CHUNKS_TO_LOAD : amountChunksWithoutContent - amountChunksWithoutContentLoaded);
					Vector<QueryResult<HSuperColumn<String, String, String>>> chunksReference = cdo.getValuesWithoutContent(System.getProperty("username"), filename, amountChunksWithoutContentLoaded, amountChunksToLoad);
					
					for(QueryResult<HSuperColumn<String, String, String>> chunkReference: chunksReference) {
						//retrieves by reference
						QueryResult<HSuperColumn<String, String, String>> chunk = cdo.getValues(chunkReference.get().getSubColumnByName("pfile").getValue(), 
								chunkReference.get().getSubColumnByName("pchunk").getValue());
					
						FileUtils.storeFileLocally(BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()), Long.parseLong(chunkReference.get().getSubColumnByName("index").getValue())
								, pathToRestore + "\\" + filename);	
						
						count++;
						int prog = (int)(Math.ceil((((double)count) * 100) / amountTotalChunks));
						setProgress(prog);
					}
							
					amountChunksWithoutContentLoaded += chunksReference.size();
					chunksReference.clear();
				}
				
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
