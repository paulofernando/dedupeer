package com.dedupeer.backup;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.dedupeer.utils.FileUtils;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class BackupQueue extends Thread {
	
	private static BackupQueue instance;
	private FileUtils fileUtils = new FileUtils();
	
	/**
	 * Map to store the file name to use to deduplicate the other file. 
	 */
	private Map<String, String> deduplicateMap = new HashMap<String, String>();
	
	/**
	 * Map with a file path as key and the backup as value
	 */
	private LinkedBlockingQueue<StoredFile> backupQueue = new LinkedBlockingQueue<StoredFile>();
	
	public static BackupQueue getInstance() {
		if(instance == null) {
			instance = new BackupQueue();			
			instance.start();
		}
		return instance;
	}
	
	/**
	 * Adds a StoredFile in the queue
	 * @param storedFile The StoredFile to add in the queue
	 */
	public void addBackup(StoredFile storedFile) {
		backupQueue.add(storedFile);
	}
	
	/**
	 * Adds in the backup queue and informs the filename to compare the chunks
	 */
	public void addBackup(StoredFile storedFile, String deduplicateWith) {
		deduplicateMap.put(storedFile.getFilename(), deduplicateWith);
		addBackup(storedFile);		
	}

	@SuppressWarnings("static-access")
	@Override
	public void run() {
		for(;;) {
			try {
				
				/* Blocks the Thread until the queue has a StoredFile */
				StoredFile currentBackup = backupQueue.take();
				
				if(deduplicateMap.containsKey(currentBackup.getFilename())) {
					currentBackup.deduplicateABigFileByThrift(deduplicateMap.get(currentBackup.getFilename()), 
							Integer.parseInt(fileUtils.getPropertiesLoader().getProperties().getProperty("default.chunk.size")) * 
							Integer.parseInt(fileUtils.getPropertiesLoader().getProperties().getProperty("chunks.to.load")));
					deduplicateMap.remove(currentBackup.getFilename());
				} else {
					currentBackup.store();
				}				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}