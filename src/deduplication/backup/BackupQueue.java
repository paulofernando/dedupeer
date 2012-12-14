package deduplication.backup;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class BackupQueue extends Thread {
	
	private int maxParallelBackups = 1;
	private static BackupQueue instance;
	
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

	@Override
	public void run() {
		for(;;) {
			try {
				StoredFile currentBackup = backupQueue.take();
				if(deduplicateMap.containsKey(currentBackup.getFilename())) {
					currentBackup.deduplicate(deduplicateMap.get(currentBackup.getFilename()));
					//currentBackup.deduplicateABigFile(deduplicateMap.get(currentBackup.getFilename()), StoredFile.defaultChunkSize * 15);
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
