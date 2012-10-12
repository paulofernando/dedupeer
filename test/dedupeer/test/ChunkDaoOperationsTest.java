package dedupeer.test;
import junit.framework.TestCase;
import deduplication.dao.operation.ChunkDaoOperations;

public class ChunkDaoOperationsTest extends TestCase {

	private String key = "ae25d454ff1d414";
	private String adler32 = "45131541631315";
	private String file_id = "152";
	private String index = "0";
	private String length = "64000";
	
	public void testGetValues() {
		ChunkDaoOperations cdh = new ChunkDaoOperations("TestCluster", "Dedupeer");
		assertTrue(adler32.equals(cdh.getValues("ae25d454ff1d414", "adler32").get().getValue()));
		assertTrue(file_id.equals(cdh.getValues("ae25d454ff1d414", "file_id").get().getValue()));
		assertTrue(index.equals(cdh.getValues("ae25d454ff1d414", "index").get().getValue()));
		assertTrue(length.equals(cdh.getValues("ae25d454ff1d414", "length").get().getValue()));
	}

}
