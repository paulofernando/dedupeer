package deduplication.delta;

public interface Delta {
	
	/**
	 * Retrieves the offset of the block in the original file
	 * @return The offset of the block in the original file
	 */
	public int getOffset();
	
	/**
	 * The size of block
	 * @return The size of block
	 */
	public int getLenght();

}
