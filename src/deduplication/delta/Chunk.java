package deduplication.delta;

public class Chunk implements Delta {

	/**
	 * The bytes of the chunk
	 */
	private byte[] dataBlock;
	
	/**
	 * The initial position of the data block in the file
	 */
	private int offset;
	
	/**
	 * Creates a new instance of a chunk
	 * @param data The bytes of the chunk
	 * @param offset The initial position of the data block in the file
	 */
	public Chunk(byte[] data, int offset) {
		this.offset = offset;
		this.dataBlock = data.clone();
	}
	
	public byte[] getData() {
		return dataBlock;
	}
	
	@Override
	public int getOffset() {
		return offset;
	}

	@Override
	public int getLenght() {		
		return (dataBlock != null) ? dataBlock.length : 0;
	}

}
