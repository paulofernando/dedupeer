package deduplication.utils;

import java.io.*;

public class FileUtils {

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
	
	public static String getOnlyName(File file) {
		return (file.getName().indexOf(".") != -1 ? file.getName().substring(0, file.getName().lastIndexOf(".")) : file.getName());
	}
}
