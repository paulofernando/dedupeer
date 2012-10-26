package deduplication.gui.component;

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
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;

import deduplication.backup.BackupQueue;
import deduplication.backup.RestoreQueue;
import deduplication.backup.StoredFile;
import deduplication.dao.operation.FilesDaoOpeartion;
import deduplication.dao.operation.UserFilesDaoOperations;
import deduplication.gui.component.model.StoredFileDataModel;
import deduplication.gui.component.renderer.IconLabelRenderer;
import deduplication.gui.component.renderer.JProgressRenderer;
import deduplication.utils.FileUtils;

public class MainPanel extends JPanel {
	
	private static final long serialVersionUID = -6912344879931889592L;
	private JButton btLogin, btAdd, btCalculate, btSettings;
	private JPanel groupButtons = new JPanel();
	private BorderLayout borderLayout = new BorderLayout();
	
	private JTable table;
	private JFrame jframe;
	
	private MouseListener mouseListener;
	private ActionListener menuListener;
	
	private final String contextmenuDeduplicate = "Use it to deduplicate other file";
	private final String contextmenuRestore = "Restore";
		
	public MainPanel(final JFrame jframe) {
		this.jframe = jframe;
		
		initComponents();
		
		this.setLayout(borderLayout);		
		this.add(groupButtons, BorderLayout.PAGE_START);

		createAndAddTable();

		JButton button = new JButton("Long-Named Button 4 (PAGE_END)");
		this.add(button, BorderLayout.PAGE_END);
		
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
					List<StoredFile> listStoredFiles = ((StoredFileDataModel) table.getModel()).getStoredFileList();
					for(StoredFile sf: listStoredFiles) {
						sf.calculateStorageEconomy();
					}
					FontMetrics fontMetrics = getFontMetrics(table.getFont());					
					int lastCol = table.getColumnCount() - 1;
					table.getColumnModel().getColumn(lastCol).setMinWidth(fontMetrics.stringWidth(table.getModel().getColumnName(lastCol)) + 20);
					table.getColumnModel().getColumn(lastCol).setMaxWidth(fontMetrics.stringWidth(table.getModel().getColumnName(lastCol)) + 20);
					((StoredFileDataModel)(table.getModel())).updateAll();
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
							JOptionPane.showMessageDialog(MainPanel.this, "Feature do not implemented yet");							
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
	
	/**
	 * Add a file in the queue to backup
	 */
	private void backupIt(File fileToBackup) {
		String filename = fileToBackup.getName();
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
				
		StoredFile backup;
		int count = 1;
		while(ufdo.fileExists(System.getProperty("username"), filename)) {
			count++;
			filename = FileUtils.getOnlyName(fileToBackup.getName()) + "(" + count + ")." + FileUtils.getOnlyExtension(fileToBackup.getName());			
		}
		
		if(count == 1) {
			backup = new StoredFile(fileToBackup, "");
		} else {
			backup = new StoredFile(fileToBackup, filename, "");
		}
		
		((StoredFileDataModel) table.getModel()).addStoredFile(backup);
		BackupQueue.getInstance().addBackup(backup);
	}
	
	/**
	 * Add a file in the queue to backup, and inform a file 
	 * already stored to deduplicate this new file
	 */
	private void backupIt(File fileToBackup, String deduplicateWith) {
		StoredFile backup = new StoredFile(fileToBackup, "");
		
		((StoredFileDataModel) table.getModel()).addStoredFile(backup);
		BackupQueue.getInstance().addBackup(backup, deduplicateWith);
	}
	
	/**
	 * Adds a file in the queue to restore
	 */
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
		this.jframe.setTitle(jframe.getTitle() + " [@" + username + "]");
		
		//unlock components
		btAdd.setEnabled(true);
		btCalculate.setEnabled(true);
		btSettings.setEnabled(true);
		
		Map<String, Long> files = new FilesDaoOpeartion("TestCluster", "Dedupeer").getAllFiles(System.getProperty("username"));
		
		for(Entry<String, Long> file: files.entrySet()) {
			((StoredFileDataModel) table.getModel()).addStoredFile(
					new StoredFile((String)file.getKey(), "", (Long)file.getValue()));
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
		
		btSettings = new JButton(new ImageIcon("resources/images/settings.png"));
		btSettings.setToolTipText("Settings");
		btSettings.setEnabled(false);
		
		groupButtons.setLayout(new FlowLayout());
		
		groupButtons.add(btLogin);
		groupButtons.add(btAdd);
		groupButtons.add(btCalculate);
		groupButtons.add(btSettings);				
	}
	
	private void createAndAddTable() {
		table = new JTable(new StoredFileDataModel());
		table.setFillsViewportHeight(true);
		table.setShowVerticalLines(false);
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setRowHeight(23);
		
		FontMetrics fontMetrics = this.getFontMetrics(table.getFont());
		for(int i = 1; i < table.getColumnCount() - 1; i++) {		
			table.getColumnModel().getColumn(i).setMaxWidth(fontMetrics.stringWidth(table.getModel().getColumnName(i) + 20));
			table.getColumnModel().getColumn(i).setMinWidth(fontMetrics.stringWidth(table.getModel().getColumnName(i)) + 20);
		}
		
		table.getColumnModel().getColumn(table.getColumnCount() - 1).setMinWidth(0);
		table.getColumnModel().getColumn(table.getColumnCount() - 1).setMaxWidth(0);
		
		table.getColumnModel().getColumn(0).setCellRenderer(new IconLabelRenderer());
		table.getColumnModel().getColumn(1).setCellRenderer(new JProgressRenderer());
				
		JScrollPane scrollPane = new JScrollPane(table);
		this.add(scrollPane, BorderLayout.CENTER);
	}
}
