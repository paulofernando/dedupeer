package deduplication.backup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import javax.swing.JButton;
import javax.swing.JProgressBar;

import me.prettyprint.cassandra.model.QueryResultImpl;
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

public class StoredFile {
	
	public static final int defaultChunkSize = 4;
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
	
	public StoredFile(File file, JProgressBar progress, String storageEconomy) {
		this.file = file;
		this.progress = progress;
		this.storageEconomy = storageEconomy;
	}
	
	public StoredFile(File file, JProgressBar progress, String storageEconomy, long id) {
		this(file, progress, storageEconomy);
		this.id = id;
	}
	
	public StoredFile(String filename, JProgressBar progress, String storageEconomy) {
		this.filename = filename;
		this.progress = progress;
		this.storageEconomy = storageEconomy;
	}
	
	public StoredFile(String filename, JProgressBar progress, String storageEconomy, long id) {
		this(filename, progress, storageEconomy);
		this.id = id;
	}
	
	public File getFile() {
		return file;
	}
	
	public String getFilename() {
		return (file != null ? file.getName() : filename);
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
				
				String fileID = String.valueOf(System.currentTimeMillis());
				
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
				
				QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), getFilename());
								
				if(result.get() == null) { //File is not in the system yet
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
					HColumn<String, String> columnFileID = result.get().getColumns().get(1);
					HColumn<String, String> columnAmountChunks = result.get().getColumns().get(0);
					
					int amountChunk = Integer.parseInt(columnAmountChunks.getValue());
					String fileIDStored = columnFileID.getValue();
					
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
						
						//TODO comparar também o md5 quando achar um adler32 no arquivo modificado
						HColumn<String, String> columnMd5 = cdo.getValues(fileIDStored, String.valueOf(i)).get().getColumns().get(4);
												
						int index = EagleEye.searchDuplication(modFile, Integer.parseInt(columnAdler32.getValue()), lastIndex, Integer.parseInt(columnLength.getValue()));
						if(index != -1) {
							newFileChunks.put(index, new ChunksDao(String.valueOf(fileID), String.valueOf(chunk_number++), String.valueOf(index), columnLength.getValue(),
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
								newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(fileID), String.valueOf(chunk_number), 
										DigestUtils.md5Hex(newchunk), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
										String.valueOf(buffer.capacity()), newchunk));
								
								buffer.clear();
							}
							index += Integer.parseInt(newFileChunks.get(index).length); //pula, porque o chunk já foi inserido na comparação com o outro arquivo
						} else {
							if(buffer.remaining() == 0) {
								c32.check(buffer.array(), 0, buffer.capacity());
								newFileChunks.put(index - buffer.capacity(), new ChunksDao(String.valueOf(fileID), String.valueOf(chunk_number), 
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
						newFileChunks.put(index - buffer.position(), new ChunksDao(String.valueOf(fileID), String.valueOf(chunk_number), 
								DigestUtils.md5Hex(Arrays.copyOfRange(buffer.array(), 0, buffer.position())), String.valueOf(c32.getValue()), String.valueOf(index - buffer.position()), 
								String.valueOf(buffer.capacity()), Arrays.copyOfRange(buffer.array(), 0, buffer.position())));
						
						buffer.clear();
					}
					
					for(ChunksDao chunk: newFileChunks.values()) {
						cdo.insertRow(chunk);			
					}
					
					String newFilename = "2_" + getFilename();
					
					ufdo.insertRow(System.getProperty("username"), newFilename, fileID, String.valueOf(file.length()), String.valueOf(amountChunk), "?");
					fdo.insertRow(System.getProperty("username"), newFilename, fileID);
				}
								
				log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
			}
		});
		
		storageProcess.start();		
	}
	
	
	public void restore() {
		Thread restoreProcess = new Thread(new Runnable() {
			@Override
			public void run() {
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				ChunksDaoOperations cdo = new ChunksDaoOperations("TestCluster", "Dedupeer");
				FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
				
				QueryResult<HSuperColumn<String, String, String>> userFileResult = ufdo.getValues(System.getProperty("username"), getFilename());
				HColumn<String, String> columnAmountChunks = userFileResult.get().getColumns().get(0);
				HColumn<String, String> columnLength = userFileResult.get().getColumns().get(2);
				
				int amountChunk = Integer.parseInt(columnAmountChunks.getValue());
				
				ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.parseInt(columnLength.getValue()));
				
				String fileID = fdo.getFileID(System.getProperty("username"), filename);
				
				for(int i = 0; i < amountChunk; i++) {
					QueryResult<HSuperColumn<String, String, String>> result = cdo.getValues(fileID, String.valueOf(i));
					
					byteBuffer.position(Integer.parseInt(result.get().getColumns().get(2).getValue()));
					byteBuffer.put(result.get().getColumns().get(1).getValueBytes());					
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

}

