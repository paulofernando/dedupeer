package deduplication.dao;

/**
 * Representation of the Cassandra's Column Family "Chunk" 
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class ChunksDao {
	
	public String fileID = "", chunkNumber = "", md5 = "", adler32 = "", index = "", length = "", pfile = "", pchunk = "", destination = "";
	public byte[] content;
	
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
	
	public ChunksDao(String fileID, String chunkNumber, String md5, String adler32, String index, String length, byte[] content) {
		this(fileID, chunkNumber, md5, adler32, index, length, "");
		this.content = content.clone();
	}
	
	public ChunksDao(String fileID, String chunkNumber, String index, String length, String pfile, String pchunk) {
		this.fileID = fileID;
		this.chunkNumber = chunkNumber;		
		this.index = index;
		this.length = length;
		this.pfile = pfile;
		this.pchunk = pchunk;
	}
	
}
