package com.dedupeer.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import javax.swing.ImageIcon;

import org.apache.log4j.Logger;

import com.dedupeer.dao.operation.UserFilesDaoOperations;
import com.dedupeer.processing.EagleEye;


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
	private ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.parseInt(getPropertiesLoader().getProperties().getProperty("default.chunk.size")));
	
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
		return getBytesFromFile(filePath, 0, (int) file.length());
	}
	
	/**
	 * Read a piece of a file in a storage device
	 * @param filePath Path of the file in the storage device
	 * @param bytesToRead amount of bytes to read
	 * @return Bytes of the file
	 */
	public static byte[] getBytesFromFile(String filePath, int offset, int bytesToRead) {
		//TODO Change the load bytes to long
		File file = new File(filePath);
		byte[] result = new byte[bytesToRead];

		InputStream input = null;
		try {
			int totalBytesRead = 0;
			input = new BufferedInputStream(new FileInputStream(file));
			while (totalBytesRead < result.length) {
				int bytesRemaining = result.length - totalBytesRead;
				int bytesRead = input.read(result, offset + totalBytesRead,
						bytesRemaining);
				if (bytesRead > 0) {
					totalBytesRead = totalBytesRead + bytesRead;
				}
			}
		} catch (FileNotFoundException ex) {
			log.info("File not found.");
		} catch (IOException ex) {
			log.info(ex);
		} finally {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public synchronized byte[] getBytesFromFile(String filePath, long offset, int bytesToRead) {
		byte[] result = null;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(filePath, "r");
			
			if(byteBuffer.capacity() != bytesToRead) {
				byteBuffer = ByteBuffer.allocate(bytesToRead);
			}
			
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
	
	public static String getValidName(String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		
		int count = 1;
		while(ufdo.fileExists(System.getProperty("username"), filename)) {
			count++;
			filename = FileUtils.getOnlyName(filename) + "(" + count + ")." + FileUtils.getOnlyExtension(filename);			
		}
		return filename;
	}

	public static PropertiesLoader getPropertiesLoader() {
		return propertiesLoader;
	}

	public static void setPropertiesLoader(PropertiesLoader propertiesLoader) {
		FileUtils.propertiesLoader = propertiesLoader;
	}
}