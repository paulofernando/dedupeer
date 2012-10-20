package deduplication.gui.component.model;

import javax.swing.JButton;
import javax.swing.JProgressBar;

import deduplication.exception.FieldNotFoundException;

public class Backup {
	
	public static final int FILE_NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	public static final int RESTORE = 3;
	
	private String filename;
	private JProgressBar progress;
	private String storageEconomy;
	private JButton btRestore;
	
	public Backup(String filename, JProgressBar progress, String storageEconomy, JButton btRestore) {
		this.filename = filename;
		this.progress = progress;
		this.storageEconomy = storageEconomy;
		this.btRestore = btRestore;
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
	
	public Object getItem(int item) throws FieldNotFoundException {
		switch (item) {
		case FILE_NAME:
			return filename;
		case PROGRESS:
			return progress;
		case ECONOMY:
			return storageEconomy;
		case RESTORE:
			return btRestore;
		}
		throw new FieldNotFoundException();
	}
}
