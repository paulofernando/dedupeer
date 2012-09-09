package deduplication.processing.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.apache.log4j.Logger;

import deduplication.utils.FileUtils;

import java.nio.ByteBuffer;
import java.util.Vector;

public class Chunking {
	
	private static final Logger log = Logger.getLogger(Chunking.class);
	
	/**
	 * Slice a file into pieces
	 * @param file File to be sliced
	 * @param destination Destination folder of the chunks
	 * @param sizeInBytes Amount of bytes for chunk
	 */
	public static void slicingAndDicing(File file, String destination, int sizeInBytes) throws IOException {
		FileInputStream is = new FileInputStream(file);

		FileChannel fc = is.getChannel();
		ByteBuffer bb = ByteBuffer.allocate(sizeInBytes);

		int chunkCount = 0;
		byte[] bytes;
		
		new File(destination).mkdir();
		
		System.out.println("Starting the slicing and dicing...");
		long time = System.currentTimeMillis();
		while (fc.read(bb) >= 0) {
			bb.flip();
			bytes = bb.array();
			storeByteArrayToFile(bytes, destination + "chunk" + "." + chunkCount);
			chunkCount++;
			bb.clear();
		}
		System.out.println(chunkCount + " created of " + (sizeInBytes/1000) + " in " + (System.currentTimeMillis() - time) + " miliseconds");
	}

	private static void storeByteArrayToFile(byte[] bytesToSave, String path) throws IOException {
		FileOutputStream fOut = new FileOutputStream(path);
		try {
			fOut.write(bytesToSave);
		} catch (Exception ex) {
			log.error("ERROR " + ex.getMessage());
		} finally {
			fOut.close();
		}
	}
	
	/**
	 * Retrieves the chunks of the file system and stores them in a vector of bytes
	 * @param path Path of the folder with the chunks
	 * @param initalNameOfCHunk The initial name of the chunks 
	 */
	public static void restoreFile(String path, String initalNameOfCHunk) {
		Vector<byte[]> chunks = new Vector<byte[]>();
		int i = 0;
		while((new File(path + initalNameOfCHunk + "." + i)).exists()) {
			chunks.addElement(FileUtils.getBytesFromFile(path + initalNameOfCHunk + "." + i));
			i++;
		}
		
		System.out.println("---------------------------------------------> " + chunks.size() + " chunks");
	}
}
