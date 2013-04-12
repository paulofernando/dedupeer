package com.dedupeer.dao;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Constants to make the code cleaner
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class Constants {
	/**
	 * The consistency level used by the Cassandra
	 */
	public final static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
	/**
	 * Name of the Keyspace of the Dedupeer in the Cassandra
	 */
	public final static String KEYSPACE = "Dedupeer";
	/**
	 * Name of a default host to the application
	 */
	public final static String HOST = "localhost";
	/**
	 * Default port to connect with Cassandra
	 */
	public final static int PORT = 9160;
}
