package deduplication.gui;

import java.awt.Toolkit;

import javax.swing.JFrame;

import deduplication.gui.component.MainPanel;

public class MainGUI extends JFrame {
		
	public MainGUI() {
		this.setTitle("Dedupeer");
		setup();
		add(new MainPanel());
		
		this.setVisible(true);
	}
	
	private void setup() {
		this.setSize(400, 500);
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);	    
	    setLocation((Toolkit.getDefaultToolkit().getScreenSize().width - this.getSize().width)>>1, 
	    		(Toolkit.getDefaultToolkit().getScreenSize().height - this.getSize().height)>>1);
	}
}
