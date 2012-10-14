package deduplication.dao;

/**
 * Representation of the Cassandra's Column Family "Chunk" 
 * @author Paulo Fernando
 *
 */
public class ChunksDao {
	
	public String fileID;
	public String chunkNumber;
	public String md5;
	public String adler32;	
	public String index;
	public String length;
	
	public String destination = "";
	
	public ChunksDao(String fileID, String chunkNumber, String md5, String adler32, String index, String length) {
		this.fileID = fileID;
		this.chunkNumber = chunkNumber;
		this.md5 = md5;
		this.adler32 = adler32;		
		this.index = index;
		this.length = length;
	}
	
	/**
	 * @param chunkName Path to the chunk on storage device
	 */
	public ChunksDao(String fileID, String chunkNumber, String md5, String adler32, String index, String length, String destination) {
		this(fileID, chunkNumber, md5, adler32, index, length);
		this.destination = destination;
	}
	
}
