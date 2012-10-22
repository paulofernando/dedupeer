package dedupeer.test;

import org.apache.log4j.Logger;

import junit.framework.TestCase;
import me.prettyprint.hector.api.beans.HColumn;
import deduplication.dao.operation.UserFilesDaoOperations;

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
		cdh.insertRow(key, filename, file_id, size, chunks, version);
		
		HColumn<String, String> columnChunks = cdh.getValues(key, filename).get().getColumns().get(0);
		HColumn<String, String> columnFileID = cdh.getValues(key, filename).get().getColumns().get(1);
		HColumn<String, String> columnSize = cdh.getValues(key, filename).get().getColumns().get(2);	
		HColumn<String, String> columnVersion = cdh.getValues(key, filename).get().getColumns().get(3);		
		
		log.info("chunks: " + chunks + " == " + columnChunks.getValue() + " ?");
		assertTrue(version.equals(columnChunks.getValue()));
		
		log.info("file_id: " + file_id + " == " + columnFileID.getValue() + " ?");
		assertTrue(file_id.equals(columnFileID.getValue()));
		
		log.info("size: " + size + " == " + columnSize.getValue() + " ?");
		assertTrue(size.equals(columnSize.getValue()));
		
		log.info("version: " + version + " == " + columnVersion.getValue() + " ?");
		assertTrue(version.equals(columnVersion.getValue()));		
	}
	
}
