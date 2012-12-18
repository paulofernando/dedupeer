package deduplication.utils;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

public class PropertiesLoader {
	
	private Properties properties;
	
	public PropertiesLoader() {		
		properties = new Properties();
		URL url = ClassLoader.getSystemResource("dedupeer.properties");
		try {
			properties.load(url.openStream());			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Properties getProperties() {
		return properties;
	}
}
