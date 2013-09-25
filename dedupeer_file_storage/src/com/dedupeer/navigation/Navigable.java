package com.dedupeer.navigation;

/**
 * All object that need of a navigation structure, for example, a folder and a file
 * @author Paulo Fernando (pf@paulofernando.net.br)
 *
 */
public interface Navigable {
	
	/** Opens an item. In the folder context, get in it. In the context of the file, restore it. */
	public void open();
	
	/** Moves the navigable to a new folder
	 * @param destinyFolder Folder where the navigable will be moved
	 */
	public void move(DFolder destinyFolder);
	
	/** Deletes the navigable. */
	public void delete();
}
