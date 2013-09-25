package com.dedupeer.navigation;

import com.dedupeer.gui.component.renderer.ProgressInfo;

/**
 * All object that need of a navigation structure, for example, a folder and a file
 * @author Paulo Fernando (pf@paulofernando.net.br)
 *
 */
public interface Navigable {
	
	public static final int NAME = 0;
	public static final int PROGRESS = 1;
	public static final int ECONOMY = 2;
	
	/** Opens an item. In the folder context, get in it. In the context of the file, restore it. */
	public void open();
	
	/** Moves the navigable to a new folder
	 * @param destinyFolder Folder where the navigable will be moved
	 */
	public void move(DFolder destinyFolder);
	
	/** Deletes the navigable. */
	public void delete();
	
	/** Retrieves the name of the navigable. */
	public String getName();
	
	/** Retrieves the progress info of the navigable. */
	public ProgressInfo getProgressInfo();
	
	/** Retrieves the storage economy (%) of the navigable. */
	public String getStorageEconomy();
	
	public void calculateStorageEconomy();
	
}
