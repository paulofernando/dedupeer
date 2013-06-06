package com.dedupeer.gui.component;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.FontMetrics;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;

import com.dedupeer.backup.BackupQueue;
import com.dedupeer.backup.RestoreQueue;
import com.dedupeer.backup.StoredFile;
import com.dedupeer.dao.operation.FilesDaoOpeartion;
import com.dedupeer.gui.component.dialog.SettingsDialog;
import com.dedupeer.gui.component.model.StoredFileDataModel;
import com.dedupeer.gui.component.renderer.IconLabelRenderer;
import com.dedupeer.gui.component.renderer.JProgressRenderer;
import com.dedupeer.utils.Utils;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class MainPanel extends JPanel {
	
	private static final long serialVersionUID = -6912344879931889592L;
	private JButton btLogin, btAdd, btCalculate, btAnalyze, btSettings;
	private JPanel groupButtons = new JPanel();
	private BorderLayout borderLayout = new BorderLayout();
	
	private JTable table;
	private JFrame jframe;
	public static JTextArea infoTextArea;
	
	private MouseListener mouseListener;
	private ActionListener menuListener;
	
	private final String contextmenuDeduplicate = "Use it to deduplicate other file";
	private final String contextmenuRestore = "Rehydrate";
		
	public MainPanel(final JFrame jframe) {
		this.jframe = jframe;
		
		initComponents();
		
		this.setLayout(borderLayout);		
		this.add(groupButtons, BorderLayout.PAGE_START);

		createAndAddTable();

		infoTextArea = new JTextArea();
		infoTextArea.setEditable(false);
		this.add(infoTextArea, BorderLayout.PAGE_END);
		
		registerListeners();		
	}
	
	private void registerListeners() {
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
					int result = fc.showOpenDialog(MainPanel.this);
					File fileToBackup = fc.getSelectedFile();
					
					if(result == JFileChooser.APPROVE_OPTION) {
						backupIt(fileToBackup);
					}
				}
			}
		});
		
		btCalculate.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(btCalculate.isEnabled()) {					
					List<StoredFile> listStoredFiles = ((StoredFileDataModel) table.getModel()).getStoredFileWithoutEconomyCalculated();
					for(StoredFile sf: listStoredFiles) {						
						sf.calculateStorageEconomy();													
					}
				}
			}
		});
		
		btAnalyze.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(btAnalyze.isEnabled()) {
					if(table.getSelectedRow() != -1) {
						StoredFile selectedFile = ((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow());
						selectedFile.analizeFile();		
					} else {
						JOptionPane.showMessageDialog(jframe, "One file need be selected");
					}
				}
			}
		});
		
		btSettings.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(btSettings.isEnabled()) {
					SwingUtilities.invokeLater(new Runnable(){
						@Override
						public void run() {							
							new SettingsDialog(jframe);							
						}
					});
					
				}
			}
		});
		
		mouseListener = new MouseListener() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if((e.getButton() == MouseEvent.BUTTON3) && (e.getClickCount() == 1)) {
					if(table.getRowCount() > 0) {
						Point point = e.getPoint();
						
						int row = table.rowAtPoint(point);
						table.getSelectionModel().setSelectionInterval(row, row);
											
						JPopupMenu contextmenu = new JPopupMenu();
						
						JMenuItem deduplicateMenu = new JMenuItem(contextmenuDeduplicate);
						deduplicateMenu.addActionListener(menuListener);
						contextmenu.add(deduplicateMenu);
						
						JMenuItem restoreMenu = new JMenuItem(contextmenuRestore);
						restoreMenu.addActionListener(menuListener);
						contextmenu.add(restoreMenu);
						
						contextmenu.show(e.getComponent(), e.getX(), e.getY());
					}
				} else if (e.getButton() == MouseEvent.BUTTON1) {					
					if(table.getSelectedRow() != -1) {
						StoredFile sf = ((StoredFileDataModel)(table.getModel())).getStoredFileList().get(table.getSelectedRow());
						if(sf.getSmallestChunk() != -1)
							infoTextArea.setText("Smallest chunk of '" + sf.getFilename() + "' = [" + sf.getSmallestChunk() + "]");
					}
				}
			}

			@Override
			public void mouseEntered(MouseEvent e) {}

			@Override
			public void mouseExited(MouseEvent e) {}

			@Override
			public void mousePressed(MouseEvent e) {}

			@Override
			public void mouseReleased(MouseEvent e) {}
		};
		
		menuListener = new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent event) {
				if(event.getActionCommand().equals(contextmenuDeduplicate)) {
					JFileChooser fc = new JFileChooser();
					int result = fc.showOpenDialog(MainPanel.this);
					File fileToBackup = fc.getSelectedFile();
					
					if(result == JFileChooser.APPROVE_OPTION) {
						backupIt(fileToBackup, ((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow()).getFilename());
					}
				} else if(event.getActionCommand().equals(contextmenuRestore)) {
					restoreIt(((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow()));
				}
			}
		};		
		table.addMouseListener(mouseListener);
	}
	
	/** Add a file in the queue to backup */
	private void backupIt(File fileToBackup) {
		String filename = fileToBackup.getName();
		String newFileName = Utils.getValidName(fileToBackup.getName());				
		StoredFile backup;
				
		if(filename.equals(newFileName)) {
			backup = new StoredFile(fileToBackup, "");
		} else {
			backup = new StoredFile(fileToBackup, newFileName, "");
		}
		
		((StoredFileDataModel) table.getModel()).addStoredFile(backup);
		BackupQueue.getInstance().addBackup(backup);
	}
	
	/**
	 * Add a file in the queue to backup, and inform a file 
	 * already stored to deduplicate this new file
	 */
	private void backupIt(File fileToBackup, String deduplicateWith) {
		String filename = fileToBackup.getName();
		String newFileName = Utils.getValidName(fileToBackup.getName());				
		StoredFile backup;
				
		if(filename.equals(newFileName)) {
			backup = new StoredFile(fileToBackup, "");
		} else {
			backup = new StoredFile(fileToBackup, newFileName, "");
		}
		
		((StoredFileDataModel) table.getModel()).addStoredFile(backup);
		BackupQueue.getInstance().addBackup(backup, deduplicateWith);
	}
	
	/** Adds a file in the queue to restore */
	private void restoreIt(StoredFile storedFile) {
		JFileChooser jfc = new JFileChooser();
		jfc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		jfc.showOpenDialog(this);
		File file = jfc.getSelectedFile();
		
		if(file != null) {
			storedFile.setPathToRestore(file.getAbsolutePath());		
			RestoreQueue.getInstance().addRestore(storedFile);
		}
	}

	protected void registerUser(String username) {
		System.setProperty("username", username);
		this.jframe.setTitle("Dedupeer [@" + username + "]");
		
		//uUnlock components
		btAdd.setEnabled(true);
		btCalculate.setEnabled(true);
		btAnalyze.setEnabled(true);
		btSettings.setEnabled(true);
		
		try {
			Map<String, Long> files = new FilesDaoOpeartion("TestCluster", "Dedupeer").getAllFiles(System.getProperty("username"));
			((StoredFileDataModel) table.getModel()).removeAllStoredFiles();
			for(Entry<String, Long> file: files.entrySet()) {
				((StoredFileDataModel) table.getModel()).addStoredFile(
						new StoredFile((String)file.getKey(), "", (Long)file.getValue()));
			}
		} catch (me.prettyprint.hector.api.exceptions.HectorException ex) {
			JOptionPane.showMessageDialog(this, "Apache Cassandra is not running!", "Error", JOptionPane.ERROR_MESSAGE);
		}
	}
	
	private void initComponents() {
		btLogin = new JButton(new ImageIcon("resources/images/login.png"));
		btLogin.setToolTipText("Login");
		
		btAdd = new JButton(new ImageIcon("resources/images/add.png"));
		btAdd.setToolTipText("Add a file");
		btAdd.setEnabled(false);
		
		btCalculate = new JButton(new ImageIcon("resources/images/calculate_storage_economy.png"));
		btCalculate.setToolTipText("Calculate the storage economy of the files stored");
		btCalculate.setEnabled(false);
		
		btAnalyze = new JButton(new ImageIcon("resources/images/analyze.png"));
		btAnalyze.setToolTipText("Analyze and show information about the selected file");
		btAnalyze.setEnabled(false);
		
		btSettings = new JButton(new ImageIcon("resources/images/settings.png"));
		btSettings.setToolTipText("Settings");
		btSettings.setEnabled(false);
		
		groupButtons.setLayout(new FlowLayout());
		
		groupButtons.add(btLogin);
		groupButtons.add(btAdd);
		groupButtons.add(btCalculate);
		groupButtons.add(btAnalyze);
		groupButtons.add(btSettings);				
	}
	
	private void createAndAddTable() {
		table = new JTable(new StoredFileDataModel());
		table.setFillsViewportHeight(true);
		table.setShowVerticalLines(false);
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setRowHeight(23);
		
		FontMetrics fontMetrics = this.getFontMetrics(table.getFont());
				
		table.getColumnModel().getColumn(1).setMaxWidth(fontMetrics.stringWidth(table.getModel().getColumnName(1)) + 50);
		table.getColumnModel().getColumn(1).setMinWidth(fontMetrics.stringWidth(table.getModel().getColumnName(1)) + 50);
		
		table.getColumnModel().getColumn(2).setMaxWidth(fontMetrics.stringWidth(table.getModel().getColumnName(2)) + 10);
		table.getColumnModel().getColumn(2).setMinWidth(fontMetrics.stringWidth(table.getModel().getColumnName(2)) + 10);
		
		
		table.getColumnModel().getColumn(0).setCellRenderer(new IconLabelRenderer());
		table.getColumnModel().getColumn(1).setCellRenderer(new JProgressRenderer());
		table.getColumnModel().getColumn(2).setCellRenderer(new IconLabelRenderer());
		
		JScrollPane scrollPane = new JScrollPane(table);
		this.add(scrollPane, BorderLayout.CENTER);
	}
}
