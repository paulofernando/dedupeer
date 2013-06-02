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
public class SettingsDialog {
	private JDialog dialog;
	private JPanel mainPanel;
	private JButton btSave, btCancel;
	
	private JTextField tfChunkSize;
	private JTextField tfChunksToLoad;
	private JCheckBox check;
	
	private JFrame parentFrame;
	
	public SettingsDialog(JFrame parentFrame) {
		
		this.parentFrame = parentFrame;
		
		this.dialog = new JDialog(parentFrame, "Settings", true);
		
		this.dialog.setResizable(false);
		this.dialog.getContentPane().add(createPane());
		this.dialog.pack();
		
		this.dialog.setSize(350, 120);
		
		this.dialog.setLocation(new Double((Toolkit.getDefaultToolkit().getScreenSize().getWidth() / 2) - (dialog.getWidth() / 2)).intValue(), 
				new Double((Toolkit.getDefaultToolkit().getScreenSize().getHeight() / 2) - (dialog.getHeight() / 2)).intValue());

		tfChunksToLoad.setText(FileUtils.getPropertiesLoader().getProperties().getProperty("chunks.to.load"));
		tfChunkSize.setText(FileUtils.getPropertiesLoader().getProperties().getProperty("default.chunk.size"));
		
		check.setSelected(FileUtils.getPropertiesLoader().getProperties().getProperty("calculate.all.hashes").equals("true"));
		
		this.dialog.setVisible(true);
	}
	
	protected Container createPane() {
		mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		
		JPanel centerPanel = new JPanel(new GridLayout(0, 2,5,2));
        JLabel lbChunkSize = new JLabel("Default chunk size (bytes):", JLabel.RIGHT);
        JLabel lbChunksToLoad = new JLabel("Chunks to load by time:", JLabel.RIGHT);
        tfChunkSize = new JTextField(20);
        tfChunksToLoad = new JTextField(20);
        check = new JCheckBox("Just calculate hashes of chunks with default size");

        centerPanel.add(lbChunkSize);
        centerPanel.add(tfChunkSize);
        centerPanel.add(lbChunksToLoad);
        centerPanel.add(tfChunksToLoad);
        
        mainPanel.add(centerPanel, BorderLayout.CENTER);
        mainPanel.add(check, BorderLayout.NORTH);
		
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.CENTER));
        
		btSave = new JButton("Save");
		btSave.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				FileUtils.getPropertiesLoader().getProperties().setProperty("chunks.to.load", tfChunksToLoad.getText());
				FileUtils.getPropertiesLoader().getProperties().setProperty("default.chunk.size", tfChunkSize.getText());
				FileUtils.getPropertiesLoader().getProperties().setProperty("calculate.all.hashes", check.isSelected() ? "true" : "false");
								
		        OutputStream out;
				try {
					out = new FileOutputStream(new File("dedupeer.properties"));
					FileUtils.getPropertiesLoader().getProperties().store(out, "");
				} catch (FileNotFoundException e1) {
					JOptionPane.showMessageDialog(parentFrame, "File not found", "Error", JOptionPane.ERROR_MESSAGE);
					e1.printStackTrace();
				} catch (IOException e1) {
					JOptionPane.showMessageDialog(parentFrame, "Error on write", "Error", JOptionPane.ERROR_MESSAGE);
					e1.printStackTrace();
				}
								
				close();
			}
		});
		
		btCancel = new JButton("Cancel");
		btCancel.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				
				close();
			}
		});
		
		buttons.add(btSave);
		buttons.add(btCancel);
		mainPanel.add(buttons, BorderLayout.SOUTH);
		
		return mainPanel;
	}

	protected void close() {
		this.dialog.dispose();
		this.dialog.setVisible(false);
	}

}