package deduplication.backup;

import java.util.concurrent.LinkedBlockingQueue;

public class BackupQueue extends Thread {
	
	private int maxParallelBackups = 1;
	private static BackupQueue instance;
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

	@Override
	public void run() {
		for(;;) {
			try {
				StoredFile currentBackup = backupQueue.take();
				currentBackup.store();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
