package deduplication.backup;

public interface StoredFileFeedback {
	
	public void updateProgress(int progress);
	
	public void setProgressType(int type);
	
}
