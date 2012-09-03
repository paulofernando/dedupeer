package test.java.net.collegeman.rsync;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import net.collegeman.rsync.Settings;
import net.collegeman.rsync.client.Client;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;

public class ClientTests extends TestCase {

	private static final Logger log = Logger.getLogger(ClientTests.class);
	
	private File source = null;
	private File dest = null;
	
	public void setUp() {
		// create copies of the linux kernel source code for testing and comparison
		source = createTempDir("2.0.10");
		dest = createTempDir("2.0.9");
		try {
			FileUtils.copyDirectory(new ClassPathResource("linux-kernel/2.0.10", ClientTests.class).getFile(), dest, true);
			FileUtils.copyDirectory(new ClassPathResource("linux-kernel/2.0.9", ClientTests.class).getFile(), source, true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private File createTempDir(String dir) {
		String tempRoot = System.getProperty("java.io.tmpdir");
		String sep = System.getProperty("file.separator");
		
		File testRoot = new File(tempRoot + sep + "java-rsync-tests");
		if (!testRoot.exists())
			testRoot.mkdir();
		
		File newDir = new File(testRoot.getAbsolutePath() + sep + dir);
		if (!newDir.exists())
			newDir.mkdir();
		else if (!newDir.isDirectory())
			throw new RuntimeException(String.format("Temporary path [%s] is not a directory!", newDir.getAbsolutePath()));
		
		return newDir;
	}
	
	public void tearDown() {
		// delete source copies
		try {
			FileUtils.deleteDirectory(source);
			FileUtils.deleteDirectory(dest);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void testLocally() {
		Settings s = new Settings();
		s.setSrc(source.getAbsolutePath());
		s.setDest(dest.getAbsolutePath());
		new Client(s).start();
		assertInSync(source, dest);
	}
	
	public void assertInSync(File expected, File found) {
		File[] expectedChildren = expected.listFiles();
		File[] foundChildren = found.listFiles();
		
		for (int i=0; i<expectedChildren.length; i++) {
			File e = expectedChildren[i];
			if (i >= foundChildren.length)
				throw new AssertionError(String.format("Expected [%s] found [null]", e.getAbsolutePath()));
			
			File f = foundChildren[i];
			
			if (e.isDirectory() && !f.isDirectory())
				throw new AssertionError(String.format("Expected directory [%s] found file [%s]", e.getAbsolutePath(), f.getAbsolutePath()));
			
			if (e.isDirectory()) {
				assertInSync(e, f);
			}
			else {
				try {
					assertTrue(String.format("Expected file [%s] not the same as [%s]", e.getAbsolutePath(), f.getAbsolutePath()), FileUtils.contentEquals(e, f));
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
			}
		}
		
	}
	
	public void testOverSSH() {
		
	}
	
}
