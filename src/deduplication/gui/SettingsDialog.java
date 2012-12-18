package deduplication.gui;

import java.awt.BorderLayout;
import java.awt.Checkbox;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Toolkit;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class SettingsDialog {
	private JDialog dialog;
	private JPanel mainPanel;
	private JButton btSave, btCancel;
	
	public SettingsDialog(JFrame parentFrame) {
		this.dialog = new JDialog(parentFrame, "Settings", true);
		
		this.dialog.setResizable(false);
		this.dialog.getContentPane().add(createPane());
		this.dialog.pack();
		
		this.dialog.setSize(300, 120);
		
		this.dialog.setLocation(new Double((Toolkit.getDefaultToolkit().getScreenSize().getWidth() / 2) - (dialog.getWidth() / 2)).intValue(), 
				new Double((Toolkit.getDefaultToolkit().getScreenSize().getHeight() / 2) - (dialog.getHeight() / 2)).intValue());
				
		this.dialog.setVisible(true);
	}
	
	protected Container createPane() {
		mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		
		JPanel centerPanel = new JPanel(new GridLayout(0, 2,5,2));
        JLabel lbChunkSize = new JLabel("Default chunk size:", JLabel.RIGHT);
        JLabel lbChunksToLoad = new JLabel("Chunks to load by time:", JLabel.RIGHT);
        JTextField tfChunkSize = new JTextField(20);
        JTextField tfChunksToLoad = new JTextField(20);
        JCheckBox check = new JCheckBox("Just calculate hashes of chunks with default size");

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