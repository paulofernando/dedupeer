package com.dedupeer.dao;

import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JOptionPane;

import com.dedupeer.dao.operation.FilesDaoOpeartion;
import com.dedupeer.dao.operation.UserFilesDaoOperations;
import com.dedupeer.gui.component.model.StoredFileDataModel;
import com.dedupeer.navigation.DFile;
import com.dedupeer.navigation.DFolder;

public class Login {
	
	private String username;
	private StoredFileDataModel listener;
	
	public Login(String username, StoredFileDataModel model) {
		this.username = username;
		listener = model;
		System.setProperty("username", username);
		
		loadFiles();
	}
	
	public void loadFiles() {
		try {			
			UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
			Map<String, Long> files = new FilesDaoOpeartion("TestCluster", "Dedupeer").getAllFiles(username);
			((StoredFileDataModel) listener).removeAllStoredFiles();
			for(Entry<String, Long> file: files.entrySet()) {
				((StoredFileDataModel) listener).addNavigable(
						new DFile((String)file.getKey(), "", (Long)file.getValue(), ufdo.getDefaultChunkSize(username, file.getKey())));
			}
		} catch (me.prettyprint.hector.api.exceptions.HectorException ex) {
			JOptionPane.showMessageDialog(null, "Apache Cassandra is not running!", "Error", JOptionPane.ERROR_MESSAGE);
		}
	}
}
