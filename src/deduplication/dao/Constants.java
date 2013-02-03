package deduplication.dao;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Constants to make the code cleaner
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class Constants {
	public final static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
	public final static String KEYSPACE = "Dedupeer";		
	public final static String HOST = "localhost";
	public final static int PORT = 9160;
}
