package dedupeer.test;

import org.apache.log4j.Logger;

import com.dedupeer.dao.operation.UserFilesDaoOperations;

import junit.framework.TestCase;
import me.prettyprint.hector.api.beans.HColumn;

public class UserFilesDaoOperationsTest extends TestCase {

	private static final Logger log = Logger.getLogger(UserFilesDaoOperationsTest.class);
	
	private String key = "paulofernando";
	private String filename = "lorem.txt";
	private String file_id = "152";
	private String size = "128000";
	private String chunks = "7";
	private String version = "1";
	
	public void testGetValues() {
		UserFilesDaoOperations cdh = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		cdh.insertRow(key, filename, file_id, size, chunks, version, 4);
		
		HColumn<String, String> columnChunks = cdh.getValues(key, filename).get().getSubColumnByName("chunks");
		HColumn<String, String> columnFileID = cdh.getValues(key, filename).get().getSubColumnByName("file_id");
		HColumn<String, String> columnSize = cdh.getValues(key, filename).get().getSubColumnByName("size");	
		HColumn<String, String> columnVersion = cdh.getValues(key, filename).get().getSubColumnByName("version");		
		
		log.info("chunks: " + chunks + " == " + columnChunks.getValue() + " ?");
		assertTrue(chunks.equals(columnChunks.getValue()));
		
		log.info("file_id: " + file_id + " == " + columnFileID.getValue() + " ?");
		assertTrue(file_id.equals(columnFileID.getValue()));
		
		log.info("size: " + size + " == " + columnSize.getValue() + " ?");
		assertTrue(size.equals(columnSize.getValue()));
		
		log.info("version: " + version + " == " + columnVersion.getValue() + " ?");
		assertTrue(version.equals(columnVersion.getValue()));		
	}
	
}
