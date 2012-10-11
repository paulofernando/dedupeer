package dedupeer.test;

import junit.framework.TestCase;
import me.prettyprint.hector.api.beans.HColumn;
import deduplication.dao.operation.UserFilesDaoOperations;

public class UserFilesDaoOperationsTest extends TestCase {

	private String key = "paulofernando";
	private String filename = "lorem.txt";
	private String file_id = "152";
	private String size = "128000";
	private String version = "1";
	
	public void testGetValues() {
		UserFilesDaoOperations cdh = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		cdh.insertRow(key, filename, file_id, size, version);
		
		HColumn<String, String> columnFileID = cdh.getValues(key, filename).get().getColumns().get(0);
		HColumn<String, String> columnSize = cdh.getValues(key, filename).get().getColumns().get(1);
		HColumn<String, String> columnVersion = cdh.getValues(key, filename).get().getColumns().get(2);
		
		assertTrue(file_id.equals(columnFileID.getValue()));
		assertTrue(size.equals(columnSize.getValue()));
		assertTrue(version.equals(columnVersion.getValue()));		
	}
	
}
