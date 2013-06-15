package com.dedupeer.gui.component.dialog;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Toolkit;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.dedupeer.gui.component.Legend;
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
	private long chunkWithContent;
	private long chunkWithoutContent;
	private long defaultChunkSize;
	
	private int maximumHeight = 130;
	
	public AnalyzeDialog(JFrame parentFrame, ArrayList<Range> ranges, long fileLength, String filename, 
			long chunkWithContent, long chunkWithoutContent, int defaultChunkSize) {		
		this.ranges = ranges;
		this.fileLength = fileLength;
		this.chunkWithContent = chunkWithContent;
		this.chunkWithoutContent = chunkWithoutContent;
		this.defaultChunkSize = defaultChunkSize;
		
		this.dialog = new JDialog(parentFrame, "Content Analyzer - [" + filename + "]", true);		
		this.dialog.getContentPane().add(createPane());
		this.dialog.pack();		
		this.dialog.setSize(600, maximumHeight);		
		this.dialog.setLocation(new Double((Toolkit.getDefaultToolkit().getScreenSize().getWidth() / 2) - (dialog.getWidth() / 2)).intValue(), 
				new Double((Toolkit.getDefaultToolkit().getScreenSize().getHeight() / 2) - (dialog.getHeight() / 2)).intValue());		
		this.dialog.setVisible(true);
		
	}
	
	protected Container createPane() {
		mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		
		ModificationBar modificationBar = new ModificationBar(ranges, fileLength);
		
		JPanel legendsPane = new JPanel();
		JPanel labelsPane = new JPanel();
		
		BoxLayout legends = new BoxLayout(legendsPane, BoxLayout.X_AXIS);		
		legendsPane.setLayout(legends);
		legendsPane.add(new Legend(Legend.TYPE_CHUNK));
		legendsPane.add(new JLabel("     "));
		legendsPane.add(new Legend(Legend.TYPE_REFERENCE));
		
		BoxLayout labels = new BoxLayout(labelsPane, BoxLayout.X_AXIS);
		labelsPane.setLayout(labels);
		labelsPane.add(new JLabel("Chunks with content: " + chunkWithContent));
		labelsPane.add(new JLabel("  |  References: " + chunkWithoutContent));
		labelsPane.add(new JLabel("  |  Total chunks: " + (chunkWithContent + chunkWithoutContent)));
		labelsPane.add(new JLabel("  |  Default chunk size: " + defaultChunkSize));
		
        JButton btClose = new JButton("Close");
		btClose.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {				
				close();
			}
		});
		
		mainPanel.add(modificationBar, BorderLayout.NORTH);
		mainPanel.add(legendsPane, BorderLayout.CENTER);
		mainPanel.add(labelsPane, BorderLayout.SOUTH);
		
		return mainPanel;
	}

	protected void close() {
		this.dialog.dispose();
		this.dialog.setVisible(false);
	}

}
