package com.dedupeer.gui.component.dialog;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Toolkit;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;

import com.dedupeer.gui.component.ModificationBar;
import com.dedupeer.utils.Range;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class AnalyzeDialog {
	private JDialog dialog;
	private JPanel mainPanel;
	private ArrayList<Range> ranges;
	private long fileLength;
	
	public AnalyzeDialog(JFrame parentFrame, ArrayList<Range> ranges, long fileLength) {		
		this.ranges = ranges;
		this.fileLength = fileLength;
		
		this.dialog = new JDialog(parentFrame, "Content Analyzer", true);		
		this.dialog.setResizable(false);
		this.dialog.getContentPane().add(createPane());
		this.dialog.pack();		
		this.dialog.setSize(300, 65);		
		this.dialog.setLocation(new Double((Toolkit.getDefaultToolkit().getScreenSize().getWidth() / 2) - (dialog.getWidth() / 2)).intValue(), 
				new Double((Toolkit.getDefaultToolkit().getScreenSize().getHeight() / 2) - (dialog.getHeight() / 2)).intValue());		
		this.dialog.setVisible(true);
	}
	
	protected Container createPane() {
		mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		
		ModificationBar modificationBar = new ModificationBar(ranges, fileLength);
		
        JButton btClose = new JButton("Close");
		btClose.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {				
				close();
			}
		});
		
		mainPanel.add(modificationBar, BorderLayout.CENTER);
		//mainPanel.add(btClose, BorderLayout.SOUTH);
		
		return mainPanel;
	}

	protected void close() {
		this.dialog.dispose();
		this.dialog.setVisible(false);
	}
}
