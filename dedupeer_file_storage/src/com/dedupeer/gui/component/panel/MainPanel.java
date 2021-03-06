package com.dedupeer.gui.component.panel;

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
import com.dedupeer.dao.CassandraManager;
import com.dedupeer.dao.Login;
import com.dedupeer.gui.component.dialog.SettingsDialog;
import com.dedupeer.gui.component.model.StoredFileDataModel;
import com.dedupeer.gui.component.renderer.IconLabelRenderer;
import com.dedupeer.gui.component.renderer.JProgressRenderer;
import com.dedupeer.navigation.DFile;
import com.dedupeer.navigation.Navigable;
import com.dedupeer.thrift.HashingAlgorithm;
import com.dedupeer.utils.FileUtils;
import com.dedupeer.utils.Utils;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class MainPanel extends JPanel {
	
	private static final long serialVersionUID = -6912344879931889592L;
	private JButton btLogin, btAdd, btCalculate, btSettings,
					btRehydrate, btDeduplicate, btAnalyze;
	private JPanel groupButtonsTop = new JPanel();
	private JPanel groupButtonsBottom = new JPanel();
	
	private BorderLayout borderLayout = new BorderLayout();
	
	private JTable table;
	private JFrame jframe;
	
	private MouseListener mouseListener;
	private ActionListener menuListener;
	
	private final String tooltipDeduplicate = "Use it to deduplicate other file";
	private final String tooltipRehydrate = "Rehydrate this file";
	private final String tooltipAnalyze = "Analyze and show information about the selected file";
	
	private Login login;
		
	public MainPanel(final JFrame jframe) {
		this.jframe = jframe;
		
		initComponents();
		
		this.setLayout(borderLayout);		
		this.add(groupButtonsTop, BorderLayout.PAGE_START);

		createAndAddTable();
		
		this.add(groupButtonsBottom, BorderLayout.PAGE_END);
		
		registerListeners();
		SwingUtilities.invokeLater(new Runnable() {			
			@Override
			public void run() {
				showLoginDialog();				
			}
		});
	}
	
	private void registerListeners() {
		btLogin.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				showLoginDialog();
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
					List<Navigable> listStoredFiles = ((StoredFileDataModel) table.getModel()).getStoredFileWithoutEconomyCalculated();
					for(Navigable sf: listStoredFiles) {						
						sf.calculateStorageEconomy();													
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
							new SettingsDialog(jframe, login);							
						}
					});
					
				}
			}
		});
		
		//-----------------------------------------------------------------------
		
		btRehydrate.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(btRehydrate.isEnabled()) {
					SwingUtilities.invokeLater(new Runnable(){
						@Override
						public void run() {							
							restoreIt((DFile)((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow()));				
						}
					});
					
				}
			}
		});
		
		btDeduplicate.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(btDeduplicate.isEnabled()) {
					SwingUtilities.invokeLater(new Runnable(){
						@Override
						public void run() {							
							JFileChooser fc = new JFileChooser();
							int result = fc.showOpenDialog(MainPanel.this);
							File fileToBackup = fc.getSelectedFile();
							
							if(result == JFileChooser.APPROVE_OPTION) {
								backupIt(fileToBackup, ((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow()).getName());
							}						
						}
					});
					
				}
			}
		});
		
		btAnalyze.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(btAnalyze.isEnabled()) {
					if(table.getSelectedRow() != -1) {
						DFile selectedFile = (DFile)((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow());
						selectedFile.analizeFile();		
					} else {
						JOptionPane.showMessageDialog(jframe, "One file need be selected");
					}
				}
			}
		});
		
		//-----------------------------------------------------------------------
		
		mouseListener = new MouseListener() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if((e.getButton() == MouseEvent.BUTTON3) && (e.getClickCount() == 1)) {
					if(table.getRowCount() > 0) {
						Point point = e.getPoint();
						
						int row = table.rowAtPoint(point);
						table.getSelectionModel().setSelectionInterval(row, row);
											
						JPopupMenu contextmenu = new JPopupMenu();
						
						JMenuItem deduplicateMenu = new JMenuItem(tooltipDeduplicate);
						deduplicateMenu.addActionListener(menuListener);
						contextmenu.add(deduplicateMenu);
						
						JMenuItem restoreMenu = new JMenuItem(tooltipRehydrate);
						restoreMenu.addActionListener(menuListener);
						contextmenu.add(restoreMenu);
						
						contextmenu.show(e.getComponent(), e.getX(), e.getY());
					}
				} else if (e.getButton() == MouseEvent.BUTTON1) {
					if (e.getClickCount() == 2) {
						((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow()).open();
					} else if (e.getClickCount() == 1) {
						if(table.getSelectedRow() != -1) {
							btRehydrate.setEnabled(true);
							btDeduplicate.setEnabled(true);
							btAnalyze.setEnabled(true);
						} else {
							btRehydrate.setEnabled(false);
							btDeduplicate.setEnabled(false);
							btAnalyze.setEnabled(false);
						}
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
				if(event.getActionCommand().equals(tooltipDeduplicate)) {
					JFileChooser fc = new JFileChooser();
					int result = fc.showOpenDialog(MainPanel.this);
					File fileToBackup = fc.getSelectedFile();
					
					if(result == JFileChooser.APPROVE_OPTION) {
						backupIt(fileToBackup, ((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow()).getName());
					}
				} else if(event.getActionCommand().equals(tooltipRehydrate)) {
					restoreIt((DFile)((StoredFileDataModel) table.getModel()).getStoredFileByRow(table.getSelectedRow()));
				}
			}
		};		
		table.addMouseListener(mouseListener);
	}
	
	/** Add a file in the queue to backup */
	private void backupIt(File fileToBackup) {
		String filename = fileToBackup.getName();
		String newFileName = Utils.getValidName(fileToBackup.getName());				
		DFile backup;
				
		if(filename.equals(newFileName)) {
			backup = new DFile(fileToBackup, "");
		} else {
			backup = new DFile(fileToBackup, newFileName, "");
		}
		
		((StoredFileDataModel) table.getModel()).addNavigable(backup);
		BackupQueue.getInstance().addBackup(backup);
	}
	
	/**
	 * Add a file in the queue to backup, and inform a file 
	 * already stored to deduplicate this new file
	 */
	private void backupIt(File fileToBackup, String deduplicateWith) {
		String filename = fileToBackup.getName();
		String newFileName = Utils.getValidName(fileToBackup.getName());				
		DFile backup;
				
		if(filename.equals(newFileName)) {
			backup = new DFile(fileToBackup, "");
		} else {
			backup = new DFile(fileToBackup, newFileName, "");
		}
		
		((StoredFileDataModel) table.getModel()).addNavigable(backup);
		BackupQueue.getInstance().addBackup(backup, deduplicateWith);
	}
	
	/** Adds a file in the queue to restore */
	private void restoreIt(DFile dFile) {
		JFileChooser jfc = new JFileChooser();
		jfc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		jfc.showOpenDialog(this);
		File file = jfc.getSelectedFile();
		
		if(file != null) {
			dFile.setPathToRestore(file.getAbsolutePath());		
			RestoreQueue.getInstance().addRestore(dFile);
		}
	}

	protected void registerUser(String username) {
		
		CassandraManager cm = new CassandraManager();		
		if(!cm.isDedupeerKeySpaceCreated()) {
			cm.createDedupeerDataModel();
		}
							
		this.jframe.setTitle("Dedupeer File Storage [@" + username + "]");
		
		//Unlock components
		btAdd.setEnabled(true);
		btCalculate.setEnabled(true);
		btSettings.setEnabled(true);
		
		login = new Login(username, (StoredFileDataModel)table.getModel());	
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
			
		btSettings = new JButton(new ImageIcon("resources/images/settings.png"));
		btSettings.setToolTipText("Settings");
		btSettings.setEnabled(false);
				
		groupButtonsTop.setLayout(new FlowLayout());
		
		groupButtonsTop.add(btLogin);
		groupButtonsTop.add(btAdd);
		groupButtonsTop.add(btCalculate);		
		groupButtonsTop.add(btSettings);
		
		//---------------------------------------------------------------------------------
		
		btRehydrate = new JButton(new ImageIcon("resources/images/rehydrate.png"));
		btRehydrate.setToolTipText("Rehydrate the selected file");
		btRehydrate.setEnabled(false);
		
		btDeduplicate = new JButton(new ImageIcon("resources/images/deduplicate.png"));
		btDeduplicate.setToolTipText("Use the selected file to deduplicate other file");
		btDeduplicate.setEnabled(false);

		btAnalyze = new JButton(new ImageIcon("resources/images/analyze.png"));
		btAnalyze.setToolTipText(tooltipAnalyze);
		btAnalyze.setEnabled(false);
		
		groupButtonsBottom.setLayout(new FlowLayout());
		groupButtonsBottom.add(btRehydrate);
		groupButtonsBottom.add(btDeduplicate);
		groupButtonsBottom.add(btAnalyze);
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
	
	private void showLoginDialog() {
		String username = JOptionPane.showInputDialog("Inform your username");
		if(username != null) {
			registerUser(username);
		}
	}
}