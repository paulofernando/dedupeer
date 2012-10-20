package deduplication.gui.component;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.FontMetrics;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import deduplication.gui.component.model.BackupDataModel;
import deduplication.gui.component.renderer.JButtonRenderer;
import deduplication.gui.component.renderer.JProgressRenderer;

public class MainPanel extends JPanel {
	
	private static final long serialVersionUID = -6912344879931889592L;
	private JButton btLogin, btAdd;
	private JPanel groupButtons = new JPanel();
	private BorderLayout borderLayout = new BorderLayout();
	
	private JTable table;
	
		
	public MainPanel() {
		initComponents();
		
		this.setLayout(borderLayout);		
		this.add(groupButtons, BorderLayout.PAGE_START);

		createAndAddTable();

		JButton button = new JButton("Long-Named Button 4 (PAGE_END)");
		this.add(button, BorderLayout.PAGE_END);
		
		btLogin.addMouseListener(new MouseAdapter() {			
			@Override
			public void mouseClicked(MouseEvent e) {
				System.setProperty("username", JOptionPane.showInputDialog("Inform your username"));
			}
		});
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
