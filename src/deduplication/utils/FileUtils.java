package deduplication.utils;

import java.io.*;

public class FileUtils {

	/**
	 * Read a file in a storage device
	 * @param filePath Path of the file in the storage device
	 * @return Bytes of the file
	 */
	public static byte[] getBytesFromFile(String filePath) {
		System.out.println("--------------------\nReading in binary file named : " + filePath);
		File file = new File(filePath);
		System.out.println("File size: " + file.length());
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
			System.out.println("Num bytes read: " + totalBytesRead);
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
		System.out.println("--------------------");
		return result;
	}
}
