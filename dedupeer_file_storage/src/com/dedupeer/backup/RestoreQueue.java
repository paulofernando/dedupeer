package com.dedupeer.backup;

import java.util.concurrent.LinkedBlockingQueue;

import com.dedupeer.navigation.DFile;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class RestoreQueue extends Thread {
	
	private static RestoreQueue instance;
	/** Map with a file path as key and the backup as value */
	private LinkedBlockingQueue<DFile> restoreQueue = new LinkedBlockingQueue<DFile>();
	
	public static RestoreQueue getInstance() {
		if(instance == null) {
			instance = new RestoreQueue();			
			instance.start();
		}
		return instance;
	}
	
	public void addRestore(DFile dFile) {
		restoreQueue.add(dFile);
	}

	@Override
	public void run() {
		for(;;) {
			try {
				DFile currentBackup = restoreQueue.take();
				currentBackup.rehydrate();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
