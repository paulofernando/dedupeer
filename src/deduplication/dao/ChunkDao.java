package deduplication.dao;

/**
 * Representation of the Cassandra's Column Family "Chunk" 
 * @author Paulo Fernando
 *
 */
public class ChunkDao {
	
	public String md5;
	public String adler32;
	public String fileID;
	public String index;
	public String length;
	
	public ChunkDao(String md5, String adler32, String fileID, String index, String length) {
		this.md5 = md5;
		this.adler32 = adler32;
		this.fileID = fileID;
		this.index = index;
		this.length = length;
	}
	
}
