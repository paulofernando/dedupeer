package com.dedupeer;

import javax.swing.SwingUtilities;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.dedupeer.gui.MainGUI;
import com.dedupeer.thrift.ThriftServer;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class DedupeerFileStorage {
	
	private static String defaultPartition = "E"; 
	
	public static void main (String[] args) {
		System.setProperty("defaultPartition", defaultPartition);
		
		LogManager.getRootLogger().setLevel((Level)Level.INFO);
		
		ThriftServer srv = new ThriftServer();
		Thread tSrv = new Thread(srv);
		tSrv.start();
		
		SwingUtilities.invokeLater(new Runnable(){
			@Override
			public void run() {
				new MainGUI();				
			}
		});
	}
}
