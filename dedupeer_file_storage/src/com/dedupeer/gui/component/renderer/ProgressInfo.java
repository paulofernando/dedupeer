package com.dedupeer.gui.component.renderer;

public class ProgressInfo {
	
	private int progress, type;
	
	public final static int TYPE_STORING = 0;
	public final static int TYPE_DEDUPLICATION = 1; 
	public final static int TYPE_CHUNKING = 2;
	public final static int TYPE_REHYDRATING = 3;
	public final static int TYPE_CALCULATION_STORAGE_ECONOMY = 4;
	public final static int TYPE_ANALYZING = 5;
	public final static int TYPE_WRITING = 6;
	public final static int TYPE_SEARCHING = 7;
	public final static int TYPE_RETRIEVING_INFO = 8;
	public final static int TYPE_NONE = 9;
	
	public ProgressInfo(int progress, int type) {
		this.progress = progress;
		this.type = type;
	}

	public int getProgress() {
		return progress;
	}

	public void setProgress(int progress) {
		this.progress = progress;
	}

	public int getType() {
		return type;
	}
	
	public String getTypeString() {
		switch(type) {
			case TYPE_STORING:
				return "Storing...";				
			case TYPE_DEDUPLICATION:
				return "Deduplicating...";
			case TYPE_CHUNKING:
				return "Chunking...";
			case TYPE_REHYDRATING:
				return "Rehydrating...";
			case TYPE_CALCULATION_STORAGE_ECONOMY:
				return "Calculating...";
			case TYPE_ANALYZING:
				return "Analyzing...";
			case TYPE_WRITING:
				return "Writing...";
			case TYPE_SEARCHING:
				return "Searching...";
			case TYPE_RETRIEVING_INFO:
				return "Retrieving info...";
			case TYPE_NONE:
			default:
				return "";
		}
	}

	public void setType(int type) {
		this.type = type;
	}
	
}
