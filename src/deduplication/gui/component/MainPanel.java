package deduplication.gui.component;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.FontMetrics;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;

public class MainPanel extends JPanel {
	
	private static final long serialVersionUID = -6912344879931889592L;
	private JButton btLogin, btAdd;
	private JPanel groupButtons = new JPanel();
	private BorderLayout borderLayout = new BorderLayout();
	
	private JTable table;
	String[] columnNames = {"File",
			"Progress",
            "Storage economy",
            "Restore"};
		
	public MainPanel() {
		initComponents();
		
		this.setLayout(borderLayout);		
		this.add(groupButtons, BorderLayout.PAGE_START);

		createAndAddTable();

		JButton button = new JButton("Long-Named Button 4 (PAGE_END)");
		this.add(button, BorderLayout.PAGE_END);
	}
	
	private void initComponents() {
		btLogin = new JButton(new ImageIcon("resources/images/login.png"));
		btAdd = new JButton(new ImageIcon("resources/images/add.png"));
		
		groupButtons.setLayout(new FlowLayout());
		
		groupButtons.add(btLogin);
		groupButtons.add(btAdd);
				
	}
	
	private void createAndAddTable() {
		table = new JTable(new String[5][4], columnNames);
		table.setFillsViewportHeight(true);
		table.setShowVerticalLines(false);
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setRowHeight(23);
		
		FontMetrics fontMetrics = this.getFontMetrics(table.getFont());
		for(int i = 1; i < columnNames.length; i++) {		
			table.getColumnModel().getColumn(i).setMaxWidth(fontMetrics.stringWidth(columnNames[i]) + 20);
			table.getColumnModel().getColumn(i).setMinWidth(fontMetrics.stringWidth(columnNames[i]) + 20);
		}
		
		
		JScrollPane scrollPane = new JScrollPane(table);
		this.add(scrollPane, BorderLayout.CENTER);
	}
}
