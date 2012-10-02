package deduplication.delta;

import java.io.File;

public class Chunk implements Delta {
	
	/**
	 * The initial position of the data block in the file
	 */
	private int offset;
	
	/**
	 * Index in the remote file that the data matches with the data in current file
	 */
	private int indexInRemoteFile;
	
	
	/**
	 * Size of data block
	 */
	private int length;
	
	/**
	 * Creates a new instance of a chunk
	 * @param offset The initial position of the data block in the file
	 * @param length Size of data block
	 * @param indexInRemoteFile Index in the remote file that the data matches with the data in current file
	 */
	public Chunk(int offset, int length, int indexInRemoteFile) {
		this.offset = offset;
		this.indexInRemoteFile = indexInRemoteFile;
		this.length = length;
	}

	public int getOffset() {
		return offset;
	}

	public int getLength() {		
		return length;
	}

	public int getIndexInRemoteFile() {
		return indexInRemoteFile;
	}
	
}
