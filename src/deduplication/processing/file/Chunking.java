package deduplication.processing.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.apache.log4j.Logger;
import java.nio.ByteBuffer;

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
}
