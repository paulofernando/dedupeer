package deduplication;

import javax.swing.SwingUtilities;

import org.apache.log4j.Logger;

import deduplication.gui.MainGUI;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class Main {
	
	private static final Logger log = Logger.getLogger(Main.class);
	
	private static String defaultPartition = "E"; 
	
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
