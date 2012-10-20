package deduplication.gui.component;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.FontMetrics;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.util.Vector;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;

import deduplication.backup.Backup;
import deduplication.backup.BackupQueue;
import deduplication.dao.operation.FilesDaoOpeartion;
import deduplication.gui.component.model.BackupDataModel;
import deduplication.gui.component.renderer.JButtonRenderer;
import deduplication.gui.component.renderer.JProgressRenderer;

public class MainPanel extends JPanel {
	
	private static final long serialVersionUID = -6912344879931889592L;
	private JButton btLogin, btAdd;
	private JPanel groupButtons = new JPanel();
	private BorderLayout borderLayout = new BorderLayout();
	
	private JTable table;
	private JFrame jframe;
		
	public MainPanel(final JFrame jframe) {
		this.jframe = jframe;
		
		initComponents();
		
		this.setLayout(borderLayout);		
		this.add(groupButtons, BorderLayout.PAGE_START);

		createAndAddTable();

		JButton button = new JButton("Long-Named Button 4 (PAGE_END)");
		this.add(button, BorderLayout.PAGE_END);
		
		registerButtonListeners();		
	}
	
	private void registerButtonListeners() {
		btLogin.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				String username = JOptionPane.showInputDialog("Inform your username");
				if(username != null) {
					registerUser(username);
				}
			}
		});
		
		btAdd.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(btAdd.isEnabled()) {
					JFileChooser fc = new JFileChooser();
					fc.showOpenDialog(MainPanel.this);
					File fileToBackup = fc.getSelectedFile();
					
					backupIt(fileToBackup);
				}
			}
		});
		
	}
	
	private void backupIt(File fileToBackup) {
		Backup backup = new Backup(fileToBackup, new JProgressBar(), "", new JButton(new ImageIcon("resources/images/restore.png")));
		((BackupDataModel) table.getModel()).addBackup(backup);
		BackupQueue.getInstance().addBackup(backup);
	}

	protected void registerUser(String username) {
		System.setProperty("username", username);
		this.jframe.setTitle(jframe.getTitle() + " [" + username + "]");
		
		//unlock components
		this.btAdd.setEnabled(true);
		
		Vector<String> files = new FilesDaoOpeartion("TestCluster", "Dedupeer").getAllFiles(System.getProperty("username"));
		
		for(String filename: files) {			
			((BackupDataModel) table.getModel()).addBackup(
					new Backup(filename, new JProgressBar(), "", new JButton(new ImageIcon("resources/images/restore.png"))));
		}
	}
	
	private void initComponents() {
		btLogin = new JButton(new ImageIcon("resources/images/login.png"));
		btAdd = new JButton(new ImageIcon("resources/images/add.png"));
		btAdd.setEnabled(false);
		
		groupButtons.setLayout(new FlowLayout());
		
		groupButtons.add(btLogin);
		groupButtons.add(btAdd);
				
	}
	
	private void createAndAddTable() {
		table = new JTable(new BackupDataModel());
		table.setFillsViewportHeight(true);
		table.setShowVerticalLines(false);
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setRowHeight(23);
		
		FontMetrics fontMetrics = this.getFontMetrics(table.getFont());
		for(int i = 1; i < table.getColumnCount(); i++) {		
			table.getColumnModel().getColumn(i).setMaxWidth(fontMetrics.stringWidth(table.getModel().getColumnName(i) + 20));
			table.getColumnModel().getColumn(i).setMinWidth(fontMetrics.stringWidth(table.getModel().getColumnName(i)) + 20);
		}
		
		table.getColumnModel().getColumn(1).setCellRenderer(new JProgressRenderer());
		table.getColumnModel().getColumn(3).setCellRenderer(new JButtonRenderer());
				
		JScrollPane scrollPane = new JScrollPane(table);
		this.add(scrollPane, BorderLayout.CENTER);
	}
}
