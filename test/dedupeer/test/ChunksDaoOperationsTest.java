package dedupeer.test;
import java.io.File;
import java.io.FileOutputStream;

import junit.framework.TestCase;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.log4j.Logger;

import deduplication.dao.operation.ChunksDaoOperations;
import deduplication.utils.FileUtils;

public class ChunksDaoOperationsTest extends TestCase {

	private static final Logger log = Logger.getLogger(ChunksDaoOperationsTest.class);
	
	private File file;
	
	private String file_id = "152552";
	private String chunk_number = "chunk_0";
	private String md5 = "ae25d454ff1d414edd855d";	
	private String adler32 = "451315416";	
	private String index = "0";
	private String length = "64000";
	private String content = "lorem ipsum";
	
	@Override
	protected void setUp() throws java.lang.Exception {
		try {
			file = new File("E:\\lorem.txt");
			FileOutputStream fos = new FileOutputStream(file);
			fos.write(content.getBytes());
			fos.flush();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		assertTrue(file.exists());
	}
	
	public void testGetValues() {
		ChunksDaoOperations cdh = new ChunksDaoOperations("TestCluster", "Dedupeer");
		
		cdh.insertRow(file_id, chunk_number, md5, adler32, index, length, FileUtils.getBytesFromFile("E:\\lorem.txt"));
		
		HColumn<String, String> columnAdler32 = cdh.getValues(file_id, chunk_number).get().getColumns().get(0);
		HColumn<String, String> columnContent = cdh.getValues(file_id, chunk_number).get().getColumns().get(1);
		HColumn<String, String> columnIndex = cdh.getValues(file_id, chunk_number).get().getColumns().get(2);
		HColumn<String, String> columnLength = cdh.getValues(file_id, chunk_number).get().getColumns().get(3);
		HColumn<String, String> columnMd5 = cdh.getValues(file_id, chunk_number).get().getColumns().get(4);
		
		log.info("content: " + content + " == " + columnContent.getValue() + " ?");
		assertTrue(content.equals(columnContent.getValue()));
		
		log.info("md5: " + md5 + " == " + columnMd5.getValue() + " ?");
		assertTrue(md5.equals(columnMd5.getValue()));
		
		log.info("adler32: " + adler32 + " == " + columnAdler32.getValue() + " ?");
		assertTrue(adler32.equals(columnAdler32.getValue()));
		
		log.info("index: " + index + " == " + columnIndex.getValue() + " ?");
		assertTrue(index.equals(columnIndex.getValue()));
		
		log.info("length: " + length + " == " + columnLength.getValue() + " ?");
		assertTrue(length.equals(columnLength.getValue()));
	}
	
	@Override
	protected void tearDown() throws java.lang.Exception {
		assertTrue(file.delete());
	}

}
