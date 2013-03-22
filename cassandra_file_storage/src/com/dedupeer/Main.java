package com.dedupeer;

import javax.swing.SwingUtilities;

import com.dedupeer.gui.MainGUI;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class Main {
	
	private static String defaultPartition = "D"; 
	
	public static void main (String[] args) {
		System.setProperty("defaultPartition", defaultPartition);
		
		SwingUtilities.invokeLater(new Runnable(){
			@Override
			public void run() {
				new MainGUI();				
			}
		});
	}
}
