package deduplication.dao;

/**
 * Representation of the Cassandra's Column Family "Chunk" 
 * @author Paulo Fernando
 *
 */
public class ChunksDao {
	
	public String fileID, chunkNumber, md5, adler32, index, length, pfile, pchunk, destination = "";
		
	/**
	 * @param chunkName Path to the chunk on storage device
	 */
	public ChunksDao(String fileID, String chunkNumber, String md5, String adler32, String index, String length, String destination) {
		this.fileID = fileID;
		this.chunkNumber = chunkNumber;
		this.md5 = md5;
		this.adler32 = adler32;		
		this.index = index;
		this.length = length;
		this.destination = destination;
	}
	
	public ChunksDao(String fileID, String chunkNumber, String index, String pfile, String pchunk, String destination) {
		this.fileID = fileID;
		this.chunkNumber = chunkNumber;		
		this.index = index;
		this.pfile = pfile;
		this.pchunk = pchunk;
		this.destination = destination;
	}
	
}
