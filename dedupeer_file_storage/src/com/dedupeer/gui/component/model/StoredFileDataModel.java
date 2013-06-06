package com.dedupeer.gui.component.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import javax.swing.table.AbstractTableModel;

import com.dedupeer.backup.StoredFile;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class StoredFileDataModel extends AbstractTableModel implements Observer {

	private List<StoredFile> storedFileList = new ArrayList<StoredFile>();
	
	private static final long serialVersionUID = 6620911388379308486L;
	private String[] columnNames = {"File", "Progress", "Storage economy"};
	
	@Override
	public int getColumnCount() {		
		return columnNames.length;
	}

	@Override
	public int getRowCount() {		
		return storedFileList.size();
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
		StoredFile storedFile =  storedFileList.get(rowIndex);
		if(storedFile == null) return null;
		
		switch(columnIndex) {
			case StoredFile.FILE_NAME:
				return storedFile.getFilename();
			case StoredFile.PROGRESS:
				return storedFile.getProgressInfo();
			case StoredFile.ECONOMY:
				return storedFile.getStorageEconomy();
		}
		
		return null;
	}
	
	public List<StoredFile> getStoredFileList() {
		return storedFileList;
	}
	
	/**
	 * Retrieves the list of the StoredFiles that still was not calculated the economy
	 * @return List with the StoredFiles that still was not calculated the economy
	 */
	public List<StoredFile> getStoredFileWithoutEconomyCalculated() {
		List<StoredFile> listWithoutEconomyCalculated = new ArrayList<StoredFile>();
		for(StoredFile sf: storedFileList) {
			if((sf.getStorageEconomy() == null) || (sf.getStorageEconomy().equals(""))) {
				listWithoutEconomyCalculated.add(sf);
			}
		}
		return listWithoutEconomyCalculated;
	}
	
	public StoredFile getStoredFileByRow(int row) {
		return storedFileList.get(row);
	}
	
	public void addStoredFile(StoredFile backup) {
		backup.addObserver(this);
		storedFileList.add(backup);
		fireTableRowsInserted(getRowCount() - 1, getRowCount() - 1);
	}
	
	public void removeStoredFile(StoredFile backup, int row) {
		storedFileList.remove(row);
		fireTableRowsDeleted(row, row);
	}
	
	public void removeAllStoredFiles() {
		int size = storedFileList.size();
		if(size > 0) {
			storedFileList.clear();
			fireTableRowsDeleted(0, size - 1);
		}
	}

	@Override
	public void update(Observable observable, Object obj) {	
		int row = storedFileList.indexOf(observable);
		if(row >= 0) 
			fireTableRowsUpdated(row, row);
	}

	public void updateAll() {
		fireTableStructureChanged();
	}
	
}
