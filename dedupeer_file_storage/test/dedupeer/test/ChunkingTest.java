package dedupeer.test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.dedupeer.chunking.Chunking;
import com.dedupeer.thrift.HashingAlgorithm;
import com.dedupeer.utils.FileUtils;

public class ChunkingTest extends TestCase {
	
	private static final Logger log = Logger.getLogger(ChunkingTest.class);

	private String defaultDir = System.getProperty("user.home"); 
	private int defaultChunkSize = 4;
	private File file;
	private String fileName = "lorem.txt";
	
	/**
	 * Test to verify if the file was divided correctly
	 */	
	@Test
	public void testChunking() {
		File txtFile = new File(defaultDir + ":/teste/lorem.txt");
		
		try { Chunking.slicingAndDicing(txtFile, new String(defaultDir + ":\\teste\\chunks\\"), defaultChunkSize, 0, (int)Math.ceil(((double)txtFile.length())/defaultChunkSize),String.valueOf(System.currentTimeMillis()), HashingAlgorithm.MD5, null); 
		} catch (IOException e) { e.printStackTrace(); }
		
		String path = defaultDir + ":\\teste\\chunks\\";
		String initalNameOfCHunk = FileUtils.getOnlyName(txtFile.getName()) + "_chunk";
				
		byte[] txtFileBytes = FileUtils.getBytesFromFile(txtFile.getAbsolutePath());
		
		int currentChunk = 0;
		boolean equals = true;
		
		int lastChunkSize = (int)txtFile.length() % defaultChunkSize;
		long totalChunks = (long)Math.ceil((double)txtFile.length()/(double)defaultChunkSize);
		
		while(currentChunk < totalChunks) {
			byte[] chunk = FileUtils.getBytesFromFile(path + initalNameOfCHunk + "." + currentChunk);
						
			String dividedChunk = new String(chunk);
			String originalChunk = new String(Arrays.copyOfRange(txtFileBytes, currentChunk * defaultChunkSize, 
					(currentChunk * defaultChunkSize) + (currentChunk == (totalChunks - 1) ? lastChunkSize : defaultChunkSize)));
			
			if(!dividedChunk.equals(originalChunk))  {
				equals = false;
				log.info(dividedChunk + " != " + originalChunk);
			} else {
				log.info(dividedChunk + " == " + originalChunk);
			}
			currentChunk++;		
		}
		
		assertTrue(equals);		
	}
	
	/**
	 *  Test to identify if the algorithm that divide a file is working correctly
	 */
	@Test
	public void testChunkingBasic() {
		file = new File(defaultDir + ":\\teste\\" + fileName);
		try { 
			Chunking.slicingAndDicing(file, new String(defaultDir + ":\\teste\\chunks\\"), defaultChunkSize, 0l, (int)Math.ceil(((double)file.length())/defaultChunkSize), String.valueOf(System.currentTimeMillis()), HashingAlgorithm.MD5, null); 
		} catch (IOException e) { 
			e.printStackTrace(); 
		}
		
		byte[] chunk0 = FileUtils.getBytesFromFile((new File(defaultDir + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file.getName()) + "_chunk.0")).getAbsolutePath());		
		byte[] chunk1 = FileUtils.getBytesFromFile((new File(defaultDir + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file.getName()) + "_chunk.1")).getAbsolutePath());
		byte[] chunk2 = FileUtils.getBytesFromFile((new File(defaultDir + ":\\teste\\chunks\\" + FileUtils.getOnlyName(file.getName()) + "_chunk.2")).getAbsolutePath());
		
		assertTrue(!((chunk0[chunk0.length - 1] == chunk1[0]) && (chunk1[chunk1.length - 1] == chunk2[0])));		
	}
	
}
