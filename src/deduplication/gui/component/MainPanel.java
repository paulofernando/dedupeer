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

import deduplication.backup.BackupQueue;
import deduplication.backup.RestoreQueue;
import deduplication.backup.StoredFile;
import deduplication.dao.operation.FilesDaoOpeartion;
import deduplication.gui.component.model.StoredFileDataModel;
import deduplication.gui.component.renderer.IconLabelRenderer;
import deduplication.gui.component.renderer.JProgressRenderer;

public class MainPanel extends JPanel {
	
	private static final long serialVersionUID = -6912344879931889592L;
	private JButton btLogin, btAdd;
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
					fc.showOpenDialog(MainPanel.this);
					File fileToBackup = fc.getSelectedFile();
					
					if(fileToBackup != null) {
						backupIt(fileToBackup);
					}
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
						
						String fileName = (String) (table.getValueAt(table.getSelectedRow(), 0));
						
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
					System.out.println("Deduplicate called");
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
		StoredFile backup = new StoredFile(fileToBackup, new JProgressBar(), "");
		((StoredFileDataModel) table.getModel()).addStoredFile(backup);
		BackupQueue.getInstance().addBackup(backup);
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
		this.btAdd.setEnabled(true);
		
		Map<String, Long> files = new FilesDaoOpeartion("TestCluster", "Dedupeer").getAllFiles(System.getProperty("username"));
		
		for(Entry<String, Long> file: files.entrySet()) {			
			((StoredFileDataModel) table.getModel()).addStoredFile(
					new StoredFile((String)file.getKey(), new JProgressBar(), "", (Long)file.getValue()));
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
		table = new JTable(new StoredFileDataModel());
		table.setFillsViewportHeight(true);
		table.setShowVerticalLines(false);
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setRowHeight(23);
		
		FontMetrics fontMetrics = this.getFontMetrics(table.getFont());
		for(int i = 1; i < table.getColumnCount(); i++) {		
			table.getColumnModel().getColumn(i).setMaxWidth(fontMetrics.stringWidth(table.getModel().getColumnName(i) + 20));
			table.getColumnModel().getColumn(i).setMinWidth(fontMetrics.stringWidth(table.getModel().getColumnName(i)) + 20);
		}
		
		table.getColumnModel().getColumn(0).setCellRenderer(new IconLabelRenderer());
		table.getColumnModel().getColumn(1).setCellRenderer(new JProgressRenderer());
				
		JScrollPane scrollPane = new JScrollPane(table);
		this.add(scrollPane, BorderLayout.CENTER);
	}
}
