package dedupeer.test;

import java.io.File;
import java.util.Random;

import com.dedupeer.utils.FileUtils;

public class RandomFileGenerator {
	
	public static byte[] generate(int chunkLength, int fileLength) {		
		long index = 0;
		byte[] chunk = null;
		String path = new String(System.getProperty("user.home") + System.getProperty("file.separator") +
				"dedupeer_test.dat");
		
		System.out.println("writing...");
		int amountOfChunks = (int)Math.floor((double)fileLength / chunkLength) + (fileLength % chunkLength != 0 ? 0 : + 1);
		for(int i = 0; i < amountOfChunks; i++) {
			chunk = new byte[chunkLength];
			new Random().nextBytes(chunk);
						
			FileUtils.storeFileLocally(chunk, index, new String(System.getProperty("user.home") + System.getProperty("file.separator") +
					"dedupeer_test.dat"));
			index += chunkLength;
		}
		
		if(fileLength % chunkLength != 0) {
			chunk = new byte[fileLength % chunkLength];
			new Random().nextBytes(chunk);
			
			FileUtils.storeFileLocally(chunk, index, path);
		}
		System.out.println("finished...");
		
		return FileUtils.getBytesFromFile(path);
	}
	
}
