package deduplication.backup;

import java.util.concurrent.LinkedBlockingQueue;


public class BackupQueue extends Thread {
	
	private int maxParallelBackups = 1;
	private static BackupQueue instance;
	/**
	 * Map with a file path as key and the backup as value
	 */
	private LinkedBlockingQueue<Backup> backupQueue = new LinkedBlockingQueue<Backup>();
	
	public static BackupQueue getInstance() {
		if(instance == null) {
			instance = new BackupQueue();			
			instance.start();
		}
		return instance;
	}
	
	public BackupQueue() {	
	}
	
	public void addBackup(Backup backup) {
		backupQueue.add(backup);
	}

	@Override
	public void run() {
		for(;;) {
			try {
				Backup currentBackup = backupQueue.take();
				currentBackup.storeTheFile();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
