package deduplication.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.swing.ImageIcon;

public class FileUtils {
	
	private static final int TYPE_AUDIO = 0;
	private static final int TYPE_VIDEO = 1;
	private static final int TYPE_IMAGE = 2;
	private static final int TYPE_TEXT = 3;
	private static final int TYPE_UNKNOWN = 4;
	
	private static Map<String, Integer> extensions;	
	
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
	 * Read a file in a storage device
	 * @param filePath Path of the file in the storage device
	 * @return Bytes of the file
	 */
	public static byte[] getBytesFromFile(String filePath) {
		File file = new File(filePath);
		byte[] result = new byte[(int) file.length()];

		InputStream input = null;
		try {
			int totalBytesRead = 0;
			input = new BufferedInputStream(new FileInputStream(file));
			while (totalBytesRead < result.length) {
				int bytesRemaining = result.length - totalBytesRead;
				int bytesRead = input.read(result, totalBytesRead,
						bytesRemaining);
				if (bytesRead > 0) {
					totalBytesRead = totalBytesRead + bytesRead;
				}
			}
		} catch (FileNotFoundException ex) {
			System.out.println("File not found.");
		} catch (IOException ex) {
			System.out.println(ex);
		} finally {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
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
		String extension = getOnlyExtension(filename);

		if(extension != null) {
			int filetype = extensions.get(extension);
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
	
	public static void storeFileLocally(byte[] data, String path) {
		File file = new File(path);
		FileOutputStream fous = null;
		try {
			fous = new FileOutputStream(file);
			fous.write(data);
			fous.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fous.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
