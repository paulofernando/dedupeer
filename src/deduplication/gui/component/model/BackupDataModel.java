package deduplication.gui.component.model;

import java.util.ArrayList;
import java.util.List;

import javax.swing.table.AbstractTableModel;

import deduplication.backup.Backup;

public class BackupDataModel extends AbstractTableModel {

	private List<Backup> backupList = new ArrayList<Backup>();
	
	private static final long serialVersionUID = 6620911388379308486L;
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
		return backupList.size();
	}
	
	@Override
	public boolean isCellEditable(int rowIndex, int columnIndex) {
		return false;
	};
	
	public String getColumnName(int columnIndex) {
		return columnNames[columnIndex];
	}

	@Override
	public Object getValueAt(int rowIndex, int columnIndex) {
		Backup backup =  backupList.get(rowIndex);
		if(backup == null) return null;
		
		switch(columnIndex) {
			case Backup.FILE_NAME:
				return backup.getFilename();
			case Backup.PROGRESS:
				return new Float(backup.getProgress().getPercentComplete());
			case Backup.ECONOMY:
				return backup.getStorageEconomy();
			case Backup.RESTORE:
				return new Boolean(backup.getBtRestore().isEnabled());
		}
		
		return null;
	}
	
	public List<Backup> getBackupList() {
		return backupList;
	}
	
	public void addBackup(Backup backup) {
		backupList.add(backup);
		fireTableRowsInserted(getRowCount() - 1, getRowCount() - 1);
	}
	
	public void removeBackup(Backup backup, int row) {
		backupList.remove(row);
		fireTableRowsDeleted(row, row);
	}

}
