package com.dedupeer.navigation;

import java.util.ArrayList;
import java.util.Observable;

import com.dedupeer.gui.component.renderer.ProgressInfo;

public class DFolder extends Observable implements Navigable {

	private ArrayList<Navigable> navigables;
	private String name;
	
	public DFolder(String name) {
		this.name = name;
	}
	
	@Override
	public void open() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void move(DFolder to) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ProgressInfo getProgressInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStorageEconomy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void calculateStorageEconomy() {
		// TODO Auto-generated method stub
		
	}
	
	

}
