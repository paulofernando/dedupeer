package com.dedupeer.navigation;

import java.util.ArrayList;
import java.util.Observable;

import com.dedupeer.gui.component.renderer.ProgressInfo;

public class DFolder extends Navigable {

	private ArrayList<Navigable> navigables;
		
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
	public ProgressInfo getProgressInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStorageEconomy() {
		return "";
	}

	@Override
	public void calculateStorageEconomy() {
		// TODO Auto-generated method stub
		
	}
	
	

}
