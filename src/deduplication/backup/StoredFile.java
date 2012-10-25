package deduplication.backup;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Observable;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JProgressBar;
import javax.swing.text.ChangedCharSetException;

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
import deduplication.processing.EagleEye;
import deduplication.processing.file.Chunking;
import deduplication.utils.FileUtils;

public class StoredFile extends Observable {
	
	public static final int defaultChunkSize = 32000;
	private static final Logger log = Logger.getLogger(StoredFile.class);
	
	public static final int FILE_NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	
	private File file;
	private JProgressBar progress;
	private String storageEconomy;
	private JButton btRestore;
	private String filename;
	
	private String pathToRestore;
	
	private long id = -1;
		
	public StoredFile(File file, JProgressBar progress, String storageEconomy, long id) {
		this(file, progress, storageEconomy);
		this.id = id;
	}
	
	public StoredFile(File file, JProgressBar progress, String storageEconomy) {
		this(file, file.getName(), progress, storageEconomy);
	}
	
	/**
	 * Store a file in the system with another name
	 */
	public StoredFile(File file, String newFilename, JProgressBar progress, String storageEconomy) {
		this.file = file;
		this.filename = newFilename;
		this.progress = progress;
		this.storageEconomy = storageEconomy;
	}
	
	public StoredFile(String filename, JProgressBar progress, String storageEconomy, long id) {
		this(filename, progress, storageEconomy);
		this.id = id;
	}
	
	public StoredFile(String filename, JProgressBar progress, String storageEconomy) {
		this.filename = filename;
		this.progress = progress;
		this.storageEconomy = storageEconomy;
	}
	
	public File getFile() {
		return file;
	}
	
	public String getFilename() {
		return filename;
	}

	public JProgressBar getProgress() {
		return progress;
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
			return progress;
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
						chunks = Chunking.slicingAndDicing(file, new String(System.getProperty("defaultPartition") + ":\\chunks\\"), defaultChunkSize, fileID); 
					} catch (IOException e) { 
						e.printStackTrace(); 
					}
					
					ufdo.insertRow(System.getProperty("username"), file.getName(), fileID, String.valueOf(file.length()), String.valueOf(chunks.size()), "?");
					fdo.insertRow(System.getProperty("username"), file.getName(), fileID);
					
					ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");		
					cdo.insertRows(chunks);
					
					setId(Long.parseLong(fileID));
				} else { //Other file version being stored
					deduplicate(file.getName()); //the same name
				}
								
				log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
			}
		});
		
		storageProcess.start();		
	}
	
	/**
	 * Deduplicates this file with a file passed as parameter
	 * @param filenameStored File name stored in the system to use to deduplicate this file
	 */
	public void deduplicate(String filenameStored) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
		
		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), filenameStored);				
		HColumn<String, String> columnAmountChunks = result.get().getColumns().get(0);		
		int amountChunk = Integer.parseInt(columnAmountChunks.getValue());
				
		String fileIDStored = ufdo.getValues(System.getProperty("username"), filenameStored).get().getColumns().get(1).getValue();		
		String newFileID = String.valueOf(System.currentTimeMillis());
		
		byte[] modFile = FileUtils.getBytesFromFile(file.getAbsolutePath());
		HashMap<Integer, ChunksDao> newFileChunks = new HashMap<Integer, ChunksDao>();
		int chunk_number = 0;
		
		Checksum32 c32 = new Checksum32();
		int lastIndex = 0;
		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");
		
		//Find the duplicated chunk in the system
		for(int i = 0; i < amountChunk; i++) {
			HColumn<String, String> columnAdler32 = cdo.getValues(fileIDStored, String.valueOf(i)).get().getColumns().get(0);
			HColumn<String, String> columnLength = cdo.getValues(fileIDStored, String.valueOf(i)).get().getColumns().get(3);
			
			//TODO comparar tamb�m o md5 quando achar um adler32 no arquivo modificado
			HColumn<String, String> columnMd5 = cdo.getValues(fileIDStored, String.valueOf(i)).get().getColumns().get(4);
									
			int index = EagleEye.searchDuplication(modFile, Integer.parseInt(columnAdler32.getValue()), lastIndex, Integer.parseInt(columnLength.getValue()));
			if(index != -1) {
				newFileChunks.put(index, new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number++), String.valueOf(index), columnLength.getValue(),
						fileIDStored, String.valueOf(i)));
				lastIndex = index;
			}
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
							String.valueOf(buffer.capacity()), newchunk));
					
					buffer.clear();
				}
				index += Integer.parseInt(newFileChunks.get(index).length); //pula, porque o chunk j� foi inserido na compara��o com o outro arquivo
			} else {
				if(buffer.remaining() == 0) {
					c32.check(buffer.array(), 0, buffer.capacity());
					newFileChunks.put(index - buffer.capacity(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
							DigestUtils.md5Hex(buffer.array()), String.valueOf(c32.getValue()), String.valueOf(index - buffer.capacity()), 
							String.valueOf(buffer.capacity()), buffer.array()));
					chunk_number++;
					
					buffer.clear();
				} else {
					buffer.put(modFile[index]);
					index++;
				}
			}
		}
		if(buffer.position() > 0) { //se o buffer ja tem alguns dados, cria um chunk com ele
			chunk_number++;
			newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(newFileID), String.valueOf(chunk_number), 
					DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position())), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
					String.valueOf(buffer.capacity()), Arrays.copyOfRange(buffer.array(), 0, buffer.position())));
			
			buffer.clear();
		}
		
		for(ChunksDao chunk: newFileChunks.values()) {
			cdo.insertRow(chunk);			
		}
		
		ufdo.insertRow(System.getProperty("username"), getFilename(), newFileID, String.valueOf(file.length()), String.valueOf(chunk_number + 1), "?"); //+1 because start in 0
		fdo.insertRow(System.getProperty("username"), getFilename(), newFileID);		
	}
	
	/**
	 * Retrieves the file, even though it is deduplicated
	 */
	public void restore() {
		Thread restoreProcess = new Thread(new Runnable() {
			@Override
			public void run() {
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");
								
				ByteBuffer byteBuffer = ByteBuffer.allocate(ufdo.getFileLength(System.getProperty("username"), getFilename()));
								
				Vector<QueryResult<HSuperColumn<String, String, String>>> chunksWithContent = cdo.getValuesWithContent(System.getProperty("username"), filename);
				for(QueryResult<HSuperColumn<String, String, String>> chunk: chunksWithContent) {					
					byteBuffer.position(Integer.parseInt(chunk.get().getColumns().get(2).getValue()));
					byteBuffer.put(chunk.get().getColumns().get(1).getValueBytes());					
				}
				
				Vector<QueryResult<HSuperColumn<String, String, String>>> chunksReference = cdo.getValuesWithoutContent(System.getProperty("username"), filename);
				for(QueryResult<HSuperColumn<String, String, String>> chunkReference: chunksReference) {
					//retrieves by reference
					QueryResult<HSuperColumn<String, String, String>> chunk = cdo.getValues(chunkReference.get().getColumns().get(2).getValue(), 
							chunkReference.get().getColumns().get(1).getValue());
					
					byteBuffer.position(Integer.parseInt(chunkReference.get().getColumns().get(0).getValue()));
					byteBuffer.put(chunk.get().getColumns().get(1).getValueBytes());
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

	public void setProgress(JProgressBar progress) {
		this.progress = progress;
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
		log.info("Calculating storage economy of " + getFilename() + "...");
		
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		int fileLength = ufdo.getFileLength(System.getProperty("username"), getFilename());
		
		ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");
		Vector<QueryResult<HSuperColumn<String, String, String>>> chunksWithContent = cdo.getValuesWithContent(System.getProperty("username"), getFilename());
		
		int bytesStored = 0;
		for(QueryResult<HSuperColumn<String, String, String>> chunk: chunksWithContent) {
			bytesStored += Integer.parseInt(chunk.get().getColumns().get(3).getValue());					
		}
		
		setStorageEconomy((100 - ((bytesStored * 100) / fileLength)) + "%");
		log.info("Storage economy of " + getFilename() + " = " + getStorageEconomy());
	}
	
}
