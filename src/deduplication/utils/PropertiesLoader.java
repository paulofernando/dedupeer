package deduplication.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class PropertiesLoader {
	
	private Properties properties;
	
	public PropertiesLoader() {		
		properties = new Properties();
		try {
			FileInputStream fis = new FileInputStream(new File("resources/dedupeer.properties"));
			properties.load(fis);			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Properties getProperties() {
		return properties;
	}
}
