package deduplication.gui.component.model;

import javax.swing.table.AbstractTableModel;


public class BackupDataModel extends AbstractTableModel {

	private String[] columnNames = {"File",
			"Progress",
            "Storage economy",
            "Restore"};
	
	@Override
	public int getColumnCount() {		
		return columnNames.length;
	}

	@Override
	public int getRowCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object getValueAt(int rowIndex, int columnIndex) {
		// TODO Auto-generated method stub
		return null;
	}

}
