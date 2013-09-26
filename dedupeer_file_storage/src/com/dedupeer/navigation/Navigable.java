package com.dedupeer.navigation;

import com.dedupeer.gui.component.renderer.ProgressInfo;
import java.util.Observable;

/**
 * All object that need of a navigation structure, for example, a folder and a file
 * @author Paulo Fernando (pf@paulofernando.net.br)
 *
 */
public abstract class Navigable extends Observable {
	
	public static final int NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	
	protected ProgressInfo progressInfo = new ProgressInfo(0, ProgressInfo.TYPE_NONE);
	protected String name;
	
	/** Opens an item. In the folder context, get in it. In the context of the file, restore it. */
	public abstract void open();
	
	
	/** Retrieves the progress info of the navigable. */
	public abstract ProgressInfo getProgressInfo();
	
	/** Retrieves the storage economy (%) of the navigable. */
	public abstract String getStorageEconomy();
	
	public abstract void calculateStorageEconomy();
	
	/** Retrieves the name of the navigable. */
	public String getName() {
		return name;
	}
	
	/** Moves the navigable to a new folder
	 * @param destinyFolder Folder where the navigable will be moved
	 */
	public void move(DFolder destinyFolder) {
		// TODO Auto-generated method stub
	}
	
	/** Deletes the navigable. */
	public void delete() {
		// TODO Auto-generated method stub
	}
		
}
