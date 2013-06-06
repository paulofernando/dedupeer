package com.dedupeer.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import javax.swing.ImageIcon;

import org.apache.log4j.Logger;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class FileUtils {
	
	private static final Logger log = Logger.getLogger(FileUtils.class);
	
	private static final int TYPE_AUDIO = 0;
	private static final int TYPE_VIDEO = 1;
	private static final int TYPE_IMAGE = 2;
	private static final int TYPE_TEXT = 3;
	private static final int TYPE_UNKNOWN = 4;
	
	private static Map<String, Integer> extensions;	
	
	private static PropertiesLoader propertiesLoader = new PropertiesLoader();
		
	static {
		extensions = new HashMap<String, Integer>();
		extensions.put("mp3", TYPE_AUDIO);
		extensions.put("m4a", TYPE_AUDIO);
		extensions.put("wma", TYPE_AUDIO);
		extensions.put("wav", TYPE_AUDIO);
		extensions.put("ac3", TYPE_AUDIO);
		extensions.put("ogg", TYPE_AUDIO);
		extensions.put("midi", TYPE_AUDIO);
		extensions.put("aiff", TYPE_AUDIO);
		extensions.put("aac", TYPE_AUDIO);
		extensions.put("flac", TYPE_AUDIO);
		
		extensions.put("mp4", TYPE_VIDEO);
		extensions.put("avi", TYPE_VIDEO);
		extensions.put("mpeg", TYPE_VIDEO);
		extensions.put("mpg", TYPE_VIDEO);
		extensions.put("vob", TYPE_VIDEO);
		extensions.put("mov", TYPE_VIDEO);
		extensions.put("mkv", TYPE_VIDEO);
		
		extensions.put("jpeg", TYPE_IMAGE);
		extensions.put("jpg", TYPE_IMAGE);
		extensions.put("png", TYPE_IMAGE);
		extensions.put("bmp", TYPE_IMAGE);
		extensions.put("tiff", TYPE_IMAGE);
		extensions.put("psd", TYPE_IMAGE);
		
		extensions.put("pdf", TYPE_TEXT);
		extensions.put("txt", TYPE_TEXT);
		extensions.put("doc", TYPE_TEXT);
		extensions.put("odd", TYPE_TEXT);
		extensions.put("rtf", TYPE_TEXT);
		extensions.put("ppt", TYPE_TEXT);
		extensions.put("xml", TYPE_TEXT);
		extensions.put("xsl", TYPE_TEXT);
		extensions.put("json", TYPE_TEXT);		
	}
	
	/**
	 * Read a whole file in a storage device
	 * @param filePath Path of the file in the storage device
	 * @return Bytes of the file
	 */
	public static byte[] getBytesFromFile(String filePath) {
		File file = new File(filePath);		
		return getBytesFromFile(filePath, 0l,  (int)file.length());
	}
	
	public synchronized static byte[] getBytesFromFile(String filePath, long offset, int bytesToRead) {
		byte[] result = null;
		ByteBuffer byteBuffer = ByteBuffer.allocate(bytesToRead);
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(filePath, "r");			
			byteBuffer.mark();
			
			FileChannel fc = raf.getChannel();
			fc.position(offset);
			fc.read(byteBuffer);
						
			result = byteBuffer.array();			
			byteBuffer.reset();		
		} catch (OutOfMemoryError e) {
			log.info("Bytes to read: " + bytesToRead);
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}
		
	public static String getOnlyName(String filename) {
		return (filename.indexOf(".") != -1 ? filename.substring(0, filename.lastIndexOf(".")) : filename);		
	}
	
	/**
	 * Extracts the extension of a file name.
	 * @param filename File name to extract the extension
	 * @return The extension of the file
	 */
	public static String getOnlyExtension(String filename) {
		String extension;
		if(filename.contains(".")) {
			extension = filename.substring(filename.lastIndexOf(".") + 1);
		} else {
			extension = filename;
		}
		return extension;
	}
	
	
	/**
	 * Create an icon with the image specific by extension
	 * @param filename Filename to retrieve an icon
	 * @return Icon
	 */
	public static ImageIcon getIconByFileType(String filename) {
		
		if((filename == "") || (!filename.contains("."))) return null;
		
		String extension = getOnlyExtension(filename);
		Integer filetype = extensions.get(extension);
		if(filetype != null) {			
			switch(filetype) {
				case TYPE_AUDIO:
					return new ImageIcon("resources/images/file_audio.png");
				case TYPE_VIDEO:
					return new ImageIcon("resources/images/file_video.png");
				case TYPE_IMAGE:
					return new ImageIcon("resources/images/file_image.png");
				case TYPE_TEXT:
					return new ImageIcon("resources/images/file_text.png");
				default:
					return new ImageIcon("resources/images/file_unknown.png");
			}
		} else {
			return new ImageIcon("resources/images/file_unknown.png");
		}
	}
	
	public static void storeFileLocally(byte[] data, long index, String path) {
		RandomAccessFile newFile = null;
		try {			
			newFile = new RandomAccessFile(new File(path), "rw");
			newFile.seek(index);
			newFile.write(data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				newFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static PropertiesLoader getPropertiesLoader() {
		return propertiesLoader;
	}

	public static void setPropertiesLoader(PropertiesLoader propertiesLoader) {
		FileUtils.propertiesLoader = propertiesLoader;
	}
	
	/**
	 * Delete the chunks created in the hard disk to store in Cassandra
	 * @param Folder path where the data were stored
	 * @param filename File name of the file that chunks were created
	 * @param filename Number of the first chunk in the folder
	 */
	public static void cleanUpChunks(String destination, String filename, int intialChunk) {		
		int chunkCount = intialChunk;
		String fname = destination + FileUtils.getOnlyName(filename) + "_chunk" + "." + chunkCount;
		while(new File(fname).delete()) {
			fname = destination + FileUtils.getOnlyName(filename) + "_chunk" + "." + (++chunkCount);
		}
	}
}