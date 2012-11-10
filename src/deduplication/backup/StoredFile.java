package deduplication.backup;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.Vector;

import javax.swing.JButton;

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
import deduplication.gui.component.MainPanel;
import deduplication.gui.component.renderer.ProgressInfo;
import deduplication.processing.EagleEye;
import deduplication.processing.file.Chunking;
import deduplication.utils.FileUtils;

public class StoredFile extends Observable implements StoredFileFeedback {
	
	public static final int defaultChunkSize = 256000;
	private static final Logger log = Logger.getLogger(StoredFile.class);
	
	public static final int FILE_NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	
	private File file;
	private ProgressInfo progressInfo = new ProgressInfo(0, ProgressInfo.TYPE_CHUNKING);
	private String storageEconomy;
	private JButton btRestore;
	private String filename;
	
	private int smallestChunk = -1;
	
	private String pathToRestore;
	
	private long id = -1;
		
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
					
					setId(Long.parseLong(fileID));
				} else { //Other file version being stored
					deduplicate(file.getName()); //the same name
				}
								
				log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
				
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
				}
			}
			setProgress((int)(((long)i * 100) / amountChunk));
		}
		
		int index = 0;
		ByteBuffer buffer = ByteBuffer.allocate(defaultChunkSize);
		while(index < modFile.length) {
			if(newFileChunks.containsKey(index)) {
				if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele
					byte[] newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
					c32.check(newchunk, 0, newchunk.length);
					newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
							DigestUtils.md5Hex(newchunk), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
							String.valueOf(newchunk.length), newchunk));
					chunk_number++;
					buffer.clear();
				}
				index += Integer.parseInt(newFileChunks.get(index).length); //pula, porque o chunk já foi inserido na comparação com o outro arquivo
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
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);
		
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
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		
		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), filenameStored);				
		HColumn<String, String> columnAmountChunks = result.get().getSubColumnByName("chunks");		
		int amountChunk = Integer.parseInt(columnAmountChunks.getValue());
				
		String fileIDStored = ufdo.getValues(System.getProperty("username"), filenameStored).get().getSubColumnByName("file_id").getValue();		
		
		//----------  Retrieving the information about the stored file -----------------		
		/** Map<adler32, Map<md5, chunkNumber>> */
		Map<Integer, Map<String, String>> fileInStorageServer = new HashMap<Integer, Map<String, String>>();		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", this);	
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
		}
		//--------------------------------------------------------------------------------
		
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
		
		for(int i = 0; i < divideInTimes; i++) {
			localIndex = 0;
			int bytesToRead;
			if(i != (divideInTimes - 1)) {
				bytesToRead = (int) Math.ceil(((double)file.length() / (double)divideInTimes));
			} else {
				if(file.length() % divideInTimes == 0) {
					bytesToRead = (int) Math.ceil(((double)file.length() / (double)divideInTimes));
				} else {
					bytesToRead = (int)(file.length() - (Math.ceil(((double)file.length() / (double)divideInTimes) * (i - 1))));
				}
				
			}
			
			byte[] modFile = FileUtils.getBytesFromFile(file.getAbsolutePath(), offset, bytesToRead);
			byte[] currentChunk = new byte[defaultChunkSize];
					
			while(localIndex < modFile.length) {				
					currentChunk = Arrays.copyOfRange(modFile, localIndex, localIndex + defaultChunkSize);
					c32.check(currentChunk, 0, currentChunk.length);
					if(fileInStorageServer.containsKey(c32.getValue())) {						
						if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele
							byte[] newchunk = Arrays.copyOfRange(buffer.array(), 0, buffer.position());
							c32.check(newchunk, 0, newchunk.length);
							newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
									DigestUtils.md5Hex(newchunk), String.valueOf(c32.getValue()), String.valueOf(globalIndex - buffer.position()), 
									String.valueOf(newchunk.length), newchunk));
							chunk_number++;
							buffer.clear();
						}
						
						String MD5 = DigestUtils.md5Hex(currentChunk);
						if(fileInStorageServer.get(c32.getValue()).containsKey(MD5)) {
							newFileChunks.put(globalIndex, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
								String.valueOf(globalIndex), String.valueOf(currentChunk.length), fileIDStored, fileInStorageServer.get(c32.getValue()).get(MD5)));
							
							chunk_number++;
							globalIndex += currentChunk.length;
							localIndex += currentChunk.length;
							
							currentChunk = Arrays.copyOfRange(modFile, localIndex, localIndex + defaultChunkSize);
							c32.check(currentChunk, 0, currentChunk.length);							
						}		
					} else {
						buffer.put(modFile[localIndex]);
						
						globalIndex++;
						localIndex++;
						c32.roll(modFile[localIndex + defaultChunkSize]);						
					}			
			}
						
			if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele			
				newFileChunks.put(globalIndex - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
						DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position())), String.valueOf(c32.getValue()), String.valueOf(globalIndex - buffer.position()), 
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
			fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);
			
			Chunking.cleanUpChunks(new String(System.getProperty("defaultPartition") + ":\\chunks\\"), getFilename());			
			
			offset += bytesToRead;
		}
		
		//last time
		
		
	}
	
	/**
	 * Retrieves the file, even though it is deduplicated
	 */
	public void rehydrate() {
		Thread restoreProcess = new Thread(new Runnable() {
			@Override
			public void run() {
				progressInfo.setType(ProgressInfo.TYPE_REHYDRATING);
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				
				ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", StoredFile.this);
								
				ByteBuffer byteBuffer = ByteBuffer.allocate(ufdo.getFileLength(System.getProperty("username"), getFilename()));
								
				Vector<QueryResult<HSuperColumn<String, String, String>>> chunksWithContent = cdo.getValuesWithContent(System.getProperty("username"), filename);
				
				long totalChunks = ufdo.getChunksCount(System.getProperty("username"), getFilename());
				long count = 0;
				
				progressInfo.setType(ProgressInfo.TYPE_WRITING);
				for(QueryResult<HSuperColumn<String, String, String>> chunk: chunksWithContent) {					
					byteBuffer.position(Integer.parseInt(chunk.get().getSubColumnByName("index").getValue()));
					byteBuffer.put(chunk.get().getSubColumnByName("content").getValueBytes());
					
					count++;					
					int prog = (int)(Math.ceil((((double)count) * 100) / totalChunks));
					setProgress(prog);
				}
				
				Vector<QueryResult<HSuperColumn<String, String, String>>> chunksReference = cdo.getValuesWithoutContent(System.getProperty("username"), filename);
				for(QueryResult<HSuperColumn<String, String, String>> chunkReference: chunksReference) {
					//retrieves by reference
					QueryResult<HSuperColumn<String, String, String>> chunk = cdo.getValues(chunkReference.get().getSubColumnByName("pfile").getValue(), 
							chunkReference.get().getSubColumnByName("pchunk").getValue());
					
					byteBuffer.position(Integer.parseInt(chunkReference.get().getSubColumnByName("index").getValue()));
					byteBuffer.put(chunk.get().getSubColumnByName("content").getValueBytes());
					
					count++;
					int prog = (int)(Math.ceil((((double)count) * 100) / totalChunks));
					setProgress(prog);
				}
				
				byteBuffer.clear();
								
				FileUtils.storeFileLocally(byteBuffer.array(), pathToRestore + "\\" + filename);				
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
				int fileLength = ufdo.getFileLength(System.getProperty("username"), getFilename());
				
				ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", StoredFile.this);
				progressInfo.setType(ProgressInfo.TYPE_CALCULATION_STORAGE_ECONOMY);
				Vector<QueryResult<HSuperColumn<String, String, String>>> chunksWithContent = cdo.getValuesWithContent(System.getProperty("username"), getFilename());
				
				long bytesStored = 0;
				smallestChunk = defaultChunkSize + 1;
				for(QueryResult<HSuperColumn<String, String, String>> chunk: chunksWithContent) {
					int bytes = Integer.parseInt(chunk.get().getSubColumnByName("length").getValue());
					bytesStored += bytes;
					if(bytes > 0)
						smallestChunk = Math.min(smallestChunk, bytes);
				}
				
				if(smallestChunk == (defaultChunkSize + 1)) { // all zero
					smallestChunk = 0;
				}
				
				setStorageEconomy((100 - ((bytesStored * 100) / fileLength)) + "%");
				log.info("Storage economy of " + getFilename() + " = " + getStorageEconomy());
				log.info("Smallest chunk of " + getFilename() + " = " + smallestChunk);
				
				progressInfo.setProgress(100);
				
				if(MainPanel.infoTextArea != null) {
					MainPanel.infoTextArea.setText("Smallest chunk of '" + getFilename() + "' = [" + smallestChunk + "]");
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

