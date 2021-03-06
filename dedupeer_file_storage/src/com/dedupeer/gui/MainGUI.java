package com.dedupeer.gui;

import java.awt.Image;
import java.awt.Toolkit;
import java.io.IOException;
import java.net.URL;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import org.pushingpixels.substance.api.SubstanceLookAndFeel;
import org.pushingpixels.substance.api.skin.OfficeSilver2007Skin;

import com.dedupeer.gui.component.panel.MainPanel;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class MainGUI extends JFrame {
	
	private static final long serialVersionUID = -1169498996174977654L;
	private final int width = 500;
	private final int height = 500;
		
	public MainGUI() {
		this.setTitle("Dedupeer File Storage");
		
		Toolkit kit = Toolkit.getDefaultToolkit();
		URL resource = getClass().getResource("/images/logo.png");
		Image img = kit.createImage(resource);
		this.setIconImage(img);
		
		setup();
		add(new MainPanel(this));
		
		pack();
		this.setVisible(true);
	}
	
	private void setup() {
		lookAndFeel();		
		this.setSize(width, height);
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);	    
	    setLocation((Toolkit.getDefaultToolkit().getScreenSize().width - this.getSize().width)>>1, 
	    		(Toolkit.getDefaultToolkit().getScreenSize().height - this.getSize().height)>>1);	    
	}	

	/** Sets up the look and feel to the application */
	private void lookAndFeel() {		
		SwingUtilities.invokeLater(new Runnable() {
	      public void run() {
	        try {
	          JFrame.setDefaultLookAndFeelDecorated(true);
	          //UIManager.setLookAndFeel("org.pushingpixels.substance.api.skin.SubstanceGraphiteLookAndFeel");
	          SubstanceLookAndFeel.setSkin(new OfficeSilver2007Skin());
	          SwingUtilities.updateComponentTreeUI(getRootPane());
	        } catch (Exception e) {
	          System.out.println("Substance failed to initialize");
	        }
	      }
	    });
	}	
}
