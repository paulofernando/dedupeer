package deduplication.backup;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public interface StoredFileFeedback {	
	public void updateProgress(int progress);	
	public void setProgressType(int type);	
}
