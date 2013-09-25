package com.dedupeer.gui.component.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import javax.swing.table.AbstractTableModel;

import com.dedupeer.navigation.DFile;
import com.dedupeer.navigation.Navigable;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class StoredFileDataModel extends AbstractTableModel implements Observer {

	private List<Navigable> navigablesList = new ArrayList<Navigable>();
	
	private static final long serialVersionUID = 6620911388379308486L;
	private String[] columnNames = {"File", "Progress", "Storage economy"};
	
	@Override
	public int getColumnCount() {		
		return columnNames.length;
	}

	@Override
	public int getRowCount() {		
		return navigablesList.size();
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
		Navigable dFile =  navigablesList.get(rowIndex);
		if(dFile == null) return null;
		
		switch(columnIndex) {
			case Navigable.NAME:
				return dFile.getName();
			case Navigable.PROGRESS:
				return dFile.getProgressInfo();
			case DFile.ECONOMY:
				return dFile.getStorageEconomy();
		}
		
		return null;
	}
	
	public List<Navigable> getStoredFileList() {
		return navigablesList;
	}
	
	/**
	 * Retrieves the list of the StoredFiles that still was not calculated the economy
	 * @return List with the StoredFiles that still was not calculated the economy
	 */
	public List<Navigable> getStoredFileWithoutEconomyCalculated() {
		List<Navigable> listWithoutEconomyCalculated = new ArrayList<Navigable>();
		for(Navigable sf: navigablesList) {
			if((sf.getStorageEconomy() == null) || (sf.getStorageEconomy().equals(""))) {
				listWithoutEconomyCalculated.add(sf);
			}
		}
		return listWithoutEconomyCalculated;
	}
	
	public Navigable getStoredFileByRow(int row) {
		return navigablesList.get(row);
	}
	
	public void addStoredFile(DFile backup) {
		backup.addObserver(this);
		navigablesList.add(backup);
		fireTableRowsInserted(getRowCount() - 1, getRowCount() - 1);
	}
	
	public void removeStoredFile(DFile backup, int row) {
		navigablesList.remove(row);
		fireTableRowsDeleted(row, row);
	}
	
	public void removeAllStoredFiles() {
		int size = navigablesList.size();
		if(size > 0) {
			navigablesList.clear();
			fireTableRowsDeleted(0, size - 1);
		}
	}

	@Override
	public void update(Observable observable, Object obj) {	
		int row = navigablesList.indexOf(observable);
		if(row >= 0) 
			fireTableRowsUpdated(row, row);
	}

	public void updateAll() {
		fireTableStructureChanged();
	}
	
}
