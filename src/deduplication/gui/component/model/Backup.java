package deduplication.gui.component.model;

import javax.swing.JButton;
import javax.swing.JProgressBar;

public class Backup {
	
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
}
