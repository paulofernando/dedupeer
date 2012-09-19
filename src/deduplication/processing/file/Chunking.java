package deduplication.processing.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;

import org.apache.log4j.Logger;

import deduplication.checksum.RollingChecksumOlder;
import deduplication.checksum.rsync.Checksum32;
import deduplication.utils.FileUtils;

public class Chunking {
	
	private static final Logger log = Logger.getLogger(Chunking.class);
	
	/**
	 * Slice a file into pieces
	 * @param file File to be sliced
	 * @param destination Destination folder of the chunks
	 * @param sizeInBytes Amount of bytes for chunk
	 */
	public static int slicingAndDicing(File file, String destination, int sizeInBytes) throws IOException {
		FileInputStream is = new FileInputStream(file);

		FileChannel fc = is.getChannel();
		ByteBuffer bb = ByteBuffer.allocate(sizeInBytes);

		int chunkCount = 0;
		byte[] bytes;
		
		new File(destination).mkdir();
		
		System.out.println("Starting the slicing and dicing...");
		long time = System.currentTimeMillis();
		String prefix = FileUtils.getOnlyName(file);
		while (fc.read(bb) >= 0) {
			bb.flip();
			bytes = bb.array();
			storeByteArrayToFile(bytes, destination + prefix + "_chunk" + "." + chunkCount);
			chunkCount++;
			bb.clear();
		}
		System.out.println(chunkCount + " created of " + (sizeInBytes/1000) + "KB in " + (System.currentTimeMillis() - time) + " miliseconds");
		return chunkCount;
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
	 * @param to Path to save the file restored
	 */
	public static void restoreFile(String path, String initalNameOfCHunk, String to) {
		Vector<byte[]> chunks = new Vector<byte[]>();
		int i = 0;
		while((new File(path + initalNameOfCHunk + "." + i)).exists()) {
			chunks.addElement(FileUtils.getBytesFromFile(path + initalNameOfCHunk + "." + i));
			i++;
		}
		
		System.out.println(chunks.size() + " chunks restored");
		new File(to.substring(0, to.lastIndexOf("\\"))).mkdir();
		write(chunks, to);
	}
	
	/**
	 * Computes the hashes of all chunks in a directory
	 * @param path Directory where the chunks are
	 * @param initalNameOfCHunk Initial name of the chunks
	 * @return Collection of hashes
	 */
	/*public static ArrayList<Long> computeHashes(String path, String initalNameOfCHunk) {
		ArrayList<Long> hashes = new ArrayList<Long>();
		
		System.out.println("Computing hashes...");
		long time = System.currentTimeMillis();
		
		int i = 0;
		while((new File(path + initalNameOfCHunk + "." + i)).exists()) {
			hashes.add(RollingChecksumOlder.sum(FileUtils.getBytesFromFile(path + initalNameOfCHunk + "." + i)));
			i++;
		}
		System.out.println("Computed hashes of " + i + " chunks in " + (System.currentTimeMillis() - time) + " miliseconds");
		return hashes;
	}*/
	public static ArrayList<Integer> computeHashes(String path, String initalNameOfCHunk) {
		ArrayList<Integer> hashes = new ArrayList<Integer>();
		
		System.out.println("Computing hashes...");
		long time = System.currentTimeMillis();
		
		Checksum32 c32 = new Checksum32();
		int i = 0;
		while((new File(path + initalNameOfCHunk + "." + i)).exists()) {
			byte[] chunk = FileUtils.getBytesFromFile(path + initalNameOfCHunk + "." + i);
			c32.check(chunk, 0, chunk.length);
			hashes.add(c32.getValue());
			i++;
		}
		System.out.println("Computed hashes of " + i + " chunks in " + (System.currentTimeMillis() - time) + " miliseconds");
		return hashes;
	}
	
	/**
	 * Write a Vector<byte[]> in a file 
	 * @param aInput Chunks to write in a file
	 * @param newFile Name of the file to create
	 */
	private static void write(Vector<byte[]> aInput, String newFile) {	    
	    try {
	      OutputStream output = null;
	      try {
	        output = new BufferedOutputStream(new FileOutputStream(newFile));
	        for(byte[] chunk: aInput) {
	        	output.write(chunk);
	        }
	      }
	      finally {
	        output.close();
	      }
	    }
	    catch(FileNotFoundException ex){
	      System.out.println("File not found.");
	    }
	    catch(IOException ex){
	    	System.out.println(ex);
	    }
	  }
}
