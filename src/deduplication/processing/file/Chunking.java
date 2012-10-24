package deduplication.processing.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import deduplication.checksum.rsync.Checksum32;
import deduplication.dao.ChunksDao;
import deduplication.utils.FileUtils;

/**
 * Utility class for operations with chunks
 * @author Paulo Fernando
 *
 */
public class Chunking {
	
	private static final Logger log = Logger.getLogger(Chunking.class);
	
	/**
	 * Slice a file into pieces
	 * @param file File to be sliced
	 * @param destination Destination folder of the chunks
	 * @param size Amount of bytes for chunk
	 * @param fileID File ID that is been stored
	 * @return chunks information and path to each chunk in hard disk 
	 */
	public static ArrayList<ChunksDao> slicingAndDicing(File file, String destination, int size, String fileID) throws IOException {
		ArrayList<ChunksDao> chunks = new ArrayList<ChunksDao>();
		
		new File(destination).mkdir();
		int filesize = (int) file.length();
		
		FileInputStream fis = new FileInputStream(file.getAbsolutePath()); 
		
		log.debug("Starting the slicing and dicing...");
		long time = System.currentTimeMillis();
		String prefix = FileUtils.getOnlyName(file.getName());
		
		byte[] b = new byte[size];
	    int ch , chunkCount = 0;

	    Checksum32 c32 = new Checksum32();
	    
	    while(filesize > 0) {
		    ch = fis.read(b,0,size);	
		
		     filesize = filesize-ch;
		
		     String fname = destination + prefix + "_chunk" + "." + chunkCount;
		     		        
		     FileOutputStream fos= new FileOutputStream(new File(fname));
		     fos.write(b,0,ch);
		     fos.flush();
		     fos.close();
		     
		     c32.check(b, 0, b.length);
		     chunks.add(new ChunksDao(fileID, String.valueOf(chunkCount), DigestUtils.md5Hex(b), String.valueOf(c32.getValue()), String.valueOf(chunkCount * b.length), String.valueOf(ch), fname));
		     
		     chunkCount++;
	    }	    	    fis.close();	
		
		log.debug(chunkCount + " created of " + (size/1000) + "KB in " + (System.currentTimeMillis() - time) + " miliseconds");
		return chunks;
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
		
		log.debug(chunks.size() + " chunks restored");
		new File(to.substring(0, to.lastIndexOf("\\"))).mkdir();
		write(chunks, to);
	}
	
	/**
	 * Computes the hashes of all chunks in a directory
	 * @param path Directory where the chunks are
	 * @param initalNameOfCHunk Initial name of the chunks
	 * @return Collection of hashes
	 */	
	public static ArrayList<Integer> computeHashes(String path, String initalNameOfCHunk) {
		ArrayList<Integer> hashes = new ArrayList<Integer>();
		
		log.debug("Computing hashes...");
		long time = System.currentTimeMillis();
		
		Checksum32 c32 = new Checksum32();
		int i = 0;
		while((new File(path + initalNameOfCHunk + "." + i)).exists()) {
			byte[] chunk = FileUtils.getBytesFromFile(path + initalNameOfCHunk + "." + i);
			c32.check(chunk, 0, chunk.length);
			hashes.add(c32.getValue());
			i++;
		}
		log.debug("Computed hashes of " + i + " chunks in " + (System.currentTimeMillis() - time) + " miliseconds");
		return hashes;
	}
	
	/**
	 * Write a Vector<byte[]> in a file 
	 * @param aInput Chunks to write in a file
	 * @param newFile Name of the file to create
	 */
	public static void write(Vector<byte[]> aInput, String newFile) {	    
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
	    	log.error("File not found.");
	    }
	    catch(IOException ex){
	    	log.error(ex);
	    }
	  }
}
