package com.dedupeer.navigation;

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
import javax.swing.SwingUtilities;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;

import com.dedupeer.backup.StoredFileFeedback;
import com.dedupeer.checksum.Checksum32;
import com.dedupeer.chunking.Chunking;
import com.dedupeer.dao.operation.ChunksDaoOperations;
import com.dedupeer.dao.operation.FilesDaoOpeartion;
import com.dedupeer.dao.operation.UserFilesDaoOperations;
import com.dedupeer.exception.FieldNotFoundException;
import com.dedupeer.gui.component.dialog.AnalyzeDialog;
import com.dedupeer.gui.component.renderer.ProgressInfo;
import com.dedupeer.processing.EagleEye;
import com.dedupeer.thrift.Chunk;
import com.dedupeer.thrift.ChunkIDs;
import com.dedupeer.thrift.DeduplicationServiceImpl;
import com.dedupeer.thrift.HashingAlgorithm;
import com.dedupeer.thrift.ThriftClient;
import com.dedupeer.utils.FileUtils;
import com.dedupeer.utils.Range;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class DFile extends Navigable implements StoredFileFeedback{
		
	private static final int CHUNKS_TO_LOAD = 20;
	
	private static final Logger log = Logger.getLogger(DFile.class);
	private FileUtils fileUtils = new FileUtils();
	public int defaultChunkSize = Integer.parseInt(FileUtils.getPropertiesLoader().getProperties().getProperty("default.chunk.size"));
		
	private File file;
	
	private String storageEconomy;
	private JButton btRestore;
	private byte[] newchunk;	
	private String pathToRestore;
	private int smallestChunk = -1;
	private long id = -1;
	
	private HashingAlgorithm hashingAlgorithm = HashingAlgorithm.SHA1;
	
	/** Indicates if the hashes of all chunks must be calculated or if only hashes of chunks with default size.
	 * Drawback if false: do not deduplicate whole identical file because do not compares all chunks */
	@SuppressWarnings("static-access")
	private boolean calculateAllHashes = fileUtils.getPropertiesLoader().getProperties().getProperty("calculate.all.hashes").equalsIgnoreCase("true");
	
	ChunksDaoOperations cdo;
	FilesDaoOpeartion fdo;
	UserFilesDaoOperations ufdo;
	
	public DFile(File file, String storageEconomy, long id) {
		this(file, storageEconomy);
		this.id = id;
	}
	
	public DFile(File file, String storageEconomy) {
		this(file, file.getName(), storageEconomy);
	}
	
	/** File with another name */
	public DFile(File file, String newFilename, String storageEconomy) {
		this.file = file;
		this.name = newFilename;
		this.storageEconomy = storageEconomy;
		
		cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", DFile.this);
		fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
	}
	
	public DFile(String filename, String storageEconomy, long id, int defaultChunkSize) {
		this.name = filename;
		this.storageEconomy = storageEconomy;
		this.id = id;
		this.defaultChunkSize = defaultChunkSize;
		
		cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", DFile.this);
		fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
	}
		
	public File getFile() {
		return file;
	}
	
	public int getProgress() {
		return progressInfo.getProgress();
	}

	@Override
	public ProgressInfo getProgressInfo() {
		return progressInfo;
	}
	
	@Override
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
			case NAME:
				return getName();
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
				progressInfo.setType(ProgressInfo.TYPE_STORING);
				
				if(!ufdo.fileExists(System.getProperty("username"), file.getName())) { //File is not in the system yet
					String fileID = String.valueOf(System.currentTimeMillis());
					ArrayList<Chunk> chunks = new ArrayList<Chunk>();
					
					
					
					int amountStoredChunks = 0;
					int chunksToLoadByTime = Integer.parseInt(fileUtils.getPropertiesLoader().getProperties().getProperty("chunks.to.load"));					
					try {
						while(amountStoredChunks < (int)Math.ceil(((double)file.length()/defaultChunkSize))) {
							chunks = Chunking.slicingAndDicing(file, new String(System.getProperty("user.home") + System.getProperty("file.separator") +
									"chunks" + System.getProperty("file.separator")), defaultChunkSize, amountStoredChunks, chunksToLoadByTime,
									fileID, hashingAlgorithm, DFile.this); 
																										
							cdo.insertRows(chunks, amountStoredChunks);
							updateProgress(1); //because the bar can fill many times in this process.
							
							setId(Long.parseLong(fileID));
														
							FileUtils.cleanUpChunks(new String(System.getProperty("user.home") + System.getProperty("file.separator") +
									"chunks" + System.getProperty("file.separator")), getName(), amountStoredChunks);
							
							amountStoredChunks += chunks.size();
							chunks.clear();
						}
						
						ufdo.insertRow(System.getProperty("username"), file.getName(), fileID, String.valueOf(file.length()), String.valueOf(amountStoredChunks), "?", defaultChunkSize);
						fdo.insertRow(System.getProperty("username"), file.getName(), fileID);
						
						UserFilesDaoOperations udo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
						udo.setAmountChunksWithContent(System.getProperty("username"), file.getName(), amountStoredChunks);
						udo.setAmountChunksWithoutContent(System.getProperty("username"), file.getName(), 0l);
						updateProgress(100);						
					} catch (IOException e) { 
						e.printStackTrace(); 
					}
				} else { //Other file version being stored
					deduplicate(file.getName());					
				} 
								
				log.info("\"" + getName() + "\" stored in " + (System.currentTimeMillis() - time) + " miliseconds");
				progressInfo.setType(ProgressInfo.TYPE_NONE);
			}
		});		
		storageProcess.start();		
	}
	
	/**
	 * Deduplicates this file with a file passed as parameter
	 * @param filenameStored File name stored in the system to use to deduplicate this file
	 * @throws HashingAlgorithmNotFound 
	 * @throws NumberFormatException 
	 */
	public void deduplicate(String filenameStored) throws NumberFormatException {		
		log.info("\n[Deduplicating...]");
		long time = System.currentTimeMillis();
		progressInfo.setType(ProgressInfo.TYPE_DEDUPLICATION);
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		
		//Default chunk size changes to the same value of the file stored
		defaultChunkSize = ufdo.getDefaultChunkSize(System.getProperty("username"), filenameStored);
		
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
			HColumn<String, String> columnAdler32 = cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("weakHash");
			HColumn<String, String> columnLength = cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("length");
			
			HColumn<String, String> columnStrongHash = cdo.getValues(fileIDStored, String.valueOf(i)).get().getSubColumnByName("strongHash");				
			
			int index = EagleEye.searchDuplication(modFile, Integer.parseInt(columnAdler32.getValue()), lastIndex + lastLength, Integer.parseInt(columnLength.getValue()));
			if(index != -1) {
				if(DeduplicationServiceImpl.getStrongHash(hashingAlgorithm, Arrays.copyOfRange(modFile, index, index + Integer.parseInt(columnLength.getValue())))
						.equals(columnStrongHash.getValue())) {
					
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
					chunk.setStrongHash(DeduplicationServiceImpl.getStrongHash(hashingAlgorithm, newchunk));
					chunk.setWeakHash(String.valueOf(c32.getValue()));
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
					chunk.setWeakHash(String.valueOf(c32.getValue()));
					chunk.setStrongHash(DeduplicationServiceImpl.getStrongHash(hashingAlgorithm, newchunk));
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
			chunk.setWeakHash(String.valueOf(c32.getValue()));
			chunk.setStrongHash(DeduplicationServiceImpl.getStrongHash(hashingAlgorithm, newchunk));
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
		
		ufdo.insertRow(System.getProperty("username"), getName(), newFileID, String.valueOf(file.length()), String.valueOf(chunk_number + 1), "?", defaultChunkSize); //+1 because start in 0
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getName(), newFileChunks.size() - referencesCount);
		ufdo.setAmountChunksWithoutContent(System.getProperty("username"), getName(), referencesCount);
		
		fdo.insertRow(System.getProperty("username"), getName(), newFileID);
		log.info("Deduplication of the file \"" + getName() + "\" finished in " + (System.currentTimeMillis() - time) + " miliseconds");
		FileUtils.cleanUpChunks(new String(System.getProperty("user.home") + System.getProperty("file.separator") +
				"chunks") + System.getProperty("file.separator"), getName(), 0);
		
		progressInfo.setType(ProgressInfo.TYPE_NONE);
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
		String newFileID;
		
		progressInfo.setType(ProgressInfo.TYPE_RETRIEVING_INFO);
		updateProgress(1);
		//----------  Retrieving the information about the stored file -----------------		
		/** Map<adler32, Map<strongHash, [FileID, chunkNumber]>> */		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", this);				
		log.info("Retrieving chunks information...");
		Map<Integer, Map<String, ChunkIDs>> chunksInStorageServer = cdo.getHashesOfAFile(fileIDStored, amountChunks);
		
		//Default chunk size changes to the same value of the file stored
		defaultChunkSize = ufdo.getDefaultChunkSize(System.getProperty("username"), filenameStored);
		//--------------------------------------------------------------------------------
		
		progressInfo.setType(ProgressInfo.TYPE_SEARCHING);		
		Map<Long,Chunk> newFileChunks = ThriftClient.getInstance().deduplicate(chunksInStorageServer, file.getAbsolutePath(), defaultChunkSize, bytesToLoadByTime, HashingAlgorithm.SHA1);
		
		progressInfo.setType(ProgressInfo.TYPE_STORING);		
		double processedChunk = 0d;
		int referencesCount = 0;
		
		for(Chunk chunk: newFileChunks.values()) {
			updateProgress((int) ((processedChunk/newFileChunks.size()) * 100));
			
			chunk.setContent(fileUtils.getBytesFromFile(file.getAbsolutePath(), Long.parseLong(chunk.index), Integer.parseInt(chunk.length)));
			
			cdo.insertRow(chunk);
			chunk.setContent(new byte[0]);
			processedChunk++;
			if(chunk.getPchunk() != null) {
				referencesCount++;
			}
		}
		
		int totalChunks = (int) processedChunk;
		newFileID = newFileChunks.get(new Long(0l)).fileID;
		ufdo.insertRow(System.getProperty("username"), getName(), newFileID, String.valueOf(file.length()), String.valueOf(totalChunks), "?", defaultChunkSize);
		ufdo.setAmountChunksWithContent(System.getProperty("username"), getName(), totalChunks - referencesCount);
		ufdo.setAmountChunksWithoutContent(System.getProperty("username"), getName(), referencesCount);
		fdo.insertRow(System.getProperty("username"), getName(), newFileID);		
		
		updateProgress(100);
		log.info("Deduplication of the file \"" + getName() + "\" finished in " + (System.currentTimeMillis() - time) + " miliseconds");
		progressInfo.setType(ProgressInfo.TYPE_NONE);
	}
	
	/** Retrieves the file, even though it is deduplicated */
	public void rehydrate() {
		Thread restoreProcess = new Thread(new Runnable() {
			@Override
			public void run() {
				long time = System.currentTimeMillis();
				progressInfo.setType(ProgressInfo.TYPE_REHYDRATING);
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");				
				ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", DFile.this);
				
				long amountTotalChunks = ufdo.getChunksCount(System.getProperty("username"), getName());				
				long amountChunksWithContent = ufdo.getChunksWithContentCount(System.getProperty("username"), getName());				
				long amountChunksWithContentLoaded = 0;
				
				progressInfo.setType(ProgressInfo.TYPE_WRITING);
				long initialChunkToLoad = 0;
				
				//--- retrieving the id ----
				HColumn<String, String> columnFileID = ufdo.getValues(System.getProperty("username"), name).get().getSubColumnByName("file_id");
				String fileID = columnFileID.getValue();				
				//-------------------------				
				
				while(amountChunksWithContent - amountChunksWithContentLoaded > 0) {					
					QueryResult<SuperSlice<String, String, String>> chunksByRange = cdo.getChunksByRange(System.getProperty("username"), 
							fileID, amountTotalChunks, initialChunkToLoad);					
										
					for(HSuperColumn<String, String, String> chunk: chunksByRange.get().getSuperColumns()) {
						if(chunk.getSubColumnByName("content") != null) {
							log.info(chunk.getName() + "[index = " + chunk.getSubColumnByName("index").getValue() + "] "/*[length = " + BytesArraySerializer.get().fromByteBuffer(chunk.getSubColumnByName("content").getValueBytes()).length + "]"*/);							
							FileUtils.storeFileLocally(BytesArraySerializer.get().fromByteBuffer(chunk.getSubColumnByName("content").getValueBytes()), Long.parseLong(chunk.getSubColumnByName("index").getValue())
									, pathToRestore + "\\" + name);							
							amountChunksWithContentLoaded++;
						}
						initialChunkToLoad++;						
					}										
					chunksByRange.get().getSuperColumns().clear();					
					DFile.this.updateProgress((int)(Math.floor((((double)amountChunksWithContentLoaded) * 100) / amountTotalChunks)));			        
				}
				
				long amountChunksWithoutContent = ufdo.getChunksWithoutContentCount(System.getProperty("username"), getName());
				
				long count = 0;
				long amountChunksWithoutContentLoaded = 0;
				initialChunkToLoad = 0;
				while(amountChunksWithoutContent - amountChunksWithoutContentLoaded > 0) {
					int amountChunksToLoad = (int)(amountChunksWithoutContent - amountChunksWithoutContentLoaded  >= CHUNKS_TO_LOAD ? CHUNKS_TO_LOAD : amountChunksWithoutContent - amountChunksWithoutContentLoaded);
					Vector<QueryResult<HSuperColumn<String, String, String>>> chunksReference = cdo.getValuesWithoutContent(System.getProperty("username"), name, initialChunkToLoad, amountChunksToLoad);
					
					for(QueryResult<HSuperColumn<String, String, String>> chunkReference: chunksReference) {
						//Retrieves by reference
						QueryResult<HSuperColumn<String, String, String>> chunk = cdo.getValues(chunkReference.get().getSubColumnByName("pfile").getValue(), 
								chunkReference.get().getSubColumnByName("pchunk").getValue());
					
						log.info(chunkReference.get().getName() + " [index = " + chunkReference.get().getSubColumnByName("index").getValue() + 
								"] [length = " + BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()).length + "]");
						FileUtils.storeFileLocally(BytesArraySerializer.get().fromByteBuffer(chunk.get().getSubColumnByName("content").getValueBytes()), Long.parseLong(chunkReference.get().getSubColumnByName("index").getValue())
								, pathToRestore + "\\" + name);	
						
						count++;
						int prog = (int)(Math.ceil((((double)(count + amountChunksWithContentLoaded)) * 100) / amountTotalChunks));
						setProgress(prog);						
						initialChunkToLoad = Long.parseLong(chunkReference.get().getName());
					}
					
					if(chunksReference.size() > 0) 
						initialChunkToLoad++;
					
					amountChunksWithoutContentLoaded += chunksReference.size();
					chunksReference.clear();
				}
				
				setProgress(0);
				log.info("\"" + getName() + "\" rehydrated in " + (System.currentTimeMillis() - time) + " miliseconds");
				progressInfo.setType(ProgressInfo.TYPE_NONE);
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
		this.name = filename;		 
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
				log.info("Calculating storage economy of \"" + getName() + "\"...");
				
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				long fileLength = ufdo.getFileLength(System.getProperty("username"), getName());
				long amountOfChunksWithContent = ufdo.getChunksWithContentCount(System.getProperty("username"), getName());
				long totalChunks = ufdo.getChunksCount(System.getProperty("username"), getName());
				
				if(totalChunks == amountOfChunksWithContent) {					
					setStorageEconomy("0%");
					progressInfo.setProgress(100);
				} else {				
					ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer", DFile.this);
					progressInfo.setType(ProgressInfo.TYPE_CALCULATION_STORAGE_ECONOMY);					
					long bytesStored = cdo.getSpaceOccupiedByTheFile(System.getProperty("username"), getName());									
					setStorageEconomy((100 - ((bytesStored * 100) / fileLength)) + "%");
					log.info("Storage economy of \"" + getName() + "\" = " + getStorageEconomy() + " | Bytes stored: " + bytesStored + " from " + fileLength);					
					progressInfo.setProgress(100);
				}
				progressInfo.setType(ProgressInfo.TYPE_NONE);
			}
		});
		calculateProcess.start();	
	}
	
	public void analizeFile() {
		Thread analizeProcess = new Thread(new Runnable() {
			@Override
			public void run() {	
				log.info("Analyzing informtaion about file " + getName() + "...");
				progressInfo.setType(ProgressInfo.TYPE_ANALYZING);
				
				final long fileLength = ufdo.getFileLength(System.getProperty("username"), getName());		
								
				final ArrayList<Range> rangesList = (ArrayList<Range>)cdo.getAreasModified(System.getProperty("username"), getName());
				
				progressInfo.setProgress(100);
				SwingUtilities.invokeLater(new Runnable() {
					@Override
					public void run() {
						new AnalyzeDialog(null, rangesList, fileLength, getName(), 
								ufdo.getChunksWithContentCount(System.getProperty("username"), name),
								ufdo.getChunksWithoutContentCount(System.getProperty("username"), name),
								ufdo.getDefaultChunkSize(System.getProperty("username"), name));						
					}
				});
				progressInfo.setType(ProgressInfo.TYPE_NONE);
			}
		});
		analizeProcess.start();
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
		
	public HashingAlgorithm getHashingAlgorithm() {
		return hashingAlgorithm;
	}

	public void setHashingAlgorithm(HashingAlgorithm hashingAlgorithm) {
		this.hashingAlgorithm = hashingAlgorithm;
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void move(DFolder to) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub
		
	}
	
}