package deduplication.backup;

import java.util.concurrent.LinkedBlockingQueue;

public class RestoreQueue extends Thread {
	
	private int maxParallelRestores = 1;
	private static RestoreQueue instance;
	/**
	 * Map with a file path as key and the backup as value
	 */
	private LinkedBlockingQueue<StoredFile> restoreQueue = new LinkedBlockingQueue<StoredFile>();
	
	public static RestoreQueue getInstance() {
		if(instance == null) {
			instance = new RestoreQueue();			
			instance.start();
		}
		return instance;
	}
	
	public void addRestore(StoredFile storedFile) {
		restoreQueue.add(storedFile);
	}

	@Override
	public void run() {
		for(;;) {
			try {
				StoredFile currentBackup = restoreQueue.take();
				currentBackup.rehydrate();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
