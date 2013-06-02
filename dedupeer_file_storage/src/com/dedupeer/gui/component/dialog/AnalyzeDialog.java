package com.dedupeer.gui.component.dialog;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Toolkit;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import com.dedupeer.utils.FileUtils;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class AnalyzeDialog {
	private JDialog dialog;
	private JPanel mainPanel;
	private JFrame parentFrame;
	
	public AnalyzeDialog(JFrame parentFrame) {
		
		this.parentFrame = parentFrame;
		
		this.dialog = new JDialog(parentFrame, "Settings", true);
		
		this.dialog.setResizable(false);
		this.dialog.getContentPane().add(createPane());
		this.dialog.pack();
		
		this.dialog.setSize(350, 120);
		
		this.dialog.setLocation(new Double((Toolkit.getDefaultToolkit().getScreenSize().getWidth() / 2) - (dialog.getWidth() / 2)).intValue(), 
				new Double((Toolkit.getDefaultToolkit().getScreenSize().getHeight() / 2) - (dialog.getHeight() / 2)).intValue());
		
		this.dialog.setVisible(true);
	}
	
	protected Container createPane() {
		mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		        		
        JButton btCancel = new JButton("Cancel");
		btCancel.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {				
				close();
			}
		});
		
		mainPanel.add(btCancel, BorderLayout.SOUTH);
		
		return mainPanel;
	}

	protected void close() {
		this.dialog.dispose();
		this.dialog.setVisible(false);
	}
}
