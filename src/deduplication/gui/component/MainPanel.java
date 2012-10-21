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
import java.util.Vector;

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

import deduplication.backup.Backup;
import deduplication.backup.BackupQueue;
import deduplication.dao.operation.FilesDaoOpeartion;
import deduplication.gui.component.model.BackupDataModel;
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
					System.out.println("Restore called");
				}
			}
		};
		
		table.addMouseListener(mouseListener);
	}
	
	private void backupIt(File fileToBackup) {
		Backup backup = new Backup(fileToBackup, new JProgressBar(), "", new JButton(new ImageIcon("resources/images/restore.png")));
		((BackupDataModel) table.getModel()).addBackup(backup);
		BackupQueue.getInstance().addBackup(backup);
	}

	protected void registerUser(String username) {
		System.setProperty("username", username);
		this.jframe.setTitle(jframe.getTitle() + " [@" + username + "]");
		
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
		
		table.getColumnModel().getColumn(0).setCellRenderer(new IconLabelRenderer());
		table.getColumnModel().getColumn(1).setCellRenderer(new JProgressRenderer());
				
		JScrollPane scrollPane = new JScrollPane(table);
		this.add(scrollPane, BorderLayout.CENTER);
	}
}
