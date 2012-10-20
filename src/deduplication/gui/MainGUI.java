package deduplication.gui;

import java.awt.Toolkit;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

import deduplication.gui.component.MainPanel;

public class MainGUI extends JFrame {
	
	private static final long serialVersionUID = -1169498996174977654L;
	private final int width = 500;
	private final int height = 500;
	
	public MainGUI() {
		this.setTitle("Dedupeer");
		
		setup();
		add(new MainPanel());
		
		pack();
		this.setVisible(true);
	}
	
	private void setup() {
		//lookAndFeel();
		
		this.setSize(width, height);
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);	    
	    setLocation((Toolkit.getDefaultToolkit().getScreenSize().width - this.getSize().width)>>1, 
	    		(Toolkit.getDefaultToolkit().getScreenSize().height - this.getSize().height)>>1);
	    
	    
	}

	

	/**
	 * Sets up the look and feel to the application
	 */
	private void lookAndFeel() {		
        try {
          JFrame.setDefaultLookAndFeelDecorated(true);
          //UIManager.setLookAndFeel("org.pushingpixels.substance.api.skin.SubstanceGraphiteLookAndFeel");
          UIManager.setLookAndFeel("org.pushingpixels.substance.api.skin.SubstanceBusinessBlackSteelLookAndFeel");
          
          SwingUtilities.updateComponentTreeUI(this);
          this.repaint();
        } catch (Exception e) {
          System.out.println("Substance failed to initialize");
        }  
	}
}