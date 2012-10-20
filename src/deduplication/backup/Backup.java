package deduplication.backup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JProgressBar;

import me.prettyprint.cassandra.model.QueryResultImpl;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;

import deduplication.dao.ChunksDao;
import deduplication.dao.operation.ChunksDaoOperations;
import deduplication.dao.operation.FilesDaoOpeartion;
import deduplication.dao.operation.UserFilesDaoOperations;
import deduplication.exception.FieldNotFoundException;
import deduplication.processing.file.Chunking;

public class Backup {
	
	public static final int defaultChunkSize = 4;
	private static final Logger log = Logger.getLogger(Backup.class);
	
	public static final int FILE_NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	public static final int RESTORE = 3;
	
	private File file;
	private JProgressBar progress;
	private String storageEconomy;
	private JButton btRestore;
	private String filename;
		
	public Backup(File file, JProgressBar progress, String storageEconomy, JButton btRestore) {
		this.file = file;
		this.progress = progress;
		this.storageEconomy = storageEconomy;
		this.btRestore = btRestore;
	}
	
	public Backup(String filename, JProgressBar progress, String storageEconomy, JButton btRestore) {
		this.filename = filename;
		this.progress = progress;
		this.storageEconomy = storageEconomy;
		this.btRestore = btRestore;		
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
		case RESTORE:
			return btRestore;
		}
		throw new FieldNotFoundException();
	}
	
	/**
	 * Start a distribute file process between the peers in the network
	 */
	public void storeTheFile() {
		Thread storageProcess = new Thread(new Runnable() {
			
			@Override
			public void run() {
				long time = System.currentTimeMillis();
				
				String fileID = String.valueOf(System.currentTimeMillis());
				
				UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				FilesDaoOpeartion fdo = new FilesDaoOpeartion("TestCluster", "Dedupeer");
				
				QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(System.getProperty("username"), getFilename());
				HColumn<String, String> columnFileID;
				
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
				} else { //Other file version being stored
					columnFileID = result.get().getColumns().get(1);
					log.info("file exists");
				}
				
				
				
				log.info("Processed in " + (System.currentTimeMillis() - time) + " miliseconds");
			}
		});
		
		storageProcess.start();
		
	}
}

