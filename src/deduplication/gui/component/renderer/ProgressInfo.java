package deduplication.gui.component.renderer;

public class ProgressInfo {
	
	private int progress, type;
	
	public final static int TYPE_STORING = 0;
	public final static int TYPE_DEDUPLICATION = 1; 
	public final static int TYPE_CHUNKING = 2;
	public final static int TYPE_NONE = 3;
	
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
			case TYPE_NONE:
			default:
				return "";
		}
	}

	public void setType(int type) {
		this.type = type;
	}
	
	
}
