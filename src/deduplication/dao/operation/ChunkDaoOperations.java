package deduplication.dao.operation;

import java.util.ArrayList;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;

import deduplication.dao.ChunkDao;

public class ChunkDaoOperations {
	
	private static final Logger log = Logger.getLogger(ChunkDaoOperations.class);
	
	private Cluster cluster;	
	private Keyspace keyspaceOperator;
	
	/**
	 * Creates an object to manipulate the operations on the Chunk Column Family
	 * @param clusterName The cluster name from instance of Cassandra
	 * @param keyspaceName The Keyspace name where the Chunk Column Family was created 
	 */
	public ChunkDaoOperations (String clusterName, String keyspaceName) {
		cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");
		keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);
	}
	
	/**
	 * Inserts a new row on the Chunk Column Family
	 */
	public void insertRow(String key, String adler32, String fileID, String index, String length) {		
		try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
            mutator.insert(key, "Chunk", HFactory.createStringColumn("adler32", adler32));
            mutator.insert(key, "Chunk", HFactory.createStringColumn("file_id", fileID));
            mutator.insert(key, "Chunk", HFactory.createStringColumn("index", index));
            mutator.insert(key, "Chunk", HFactory.createStringColumn("length", length));
        } catch (HectorException e) {
        	log.error("Data was not inserted");
            e.printStackTrace();
        }        
	}
	
	/**
	 * Inserts a collection of chunks on the Chunk Column Family
	 */
	public void insertRows(ArrayList<ChunkDao> chunks) {
		for(ChunkDao c: chunks) {
			try {
	            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
	            mutator.insert(c.md5, "Chunk", HFactory.createStringColumn("adler32", c.adler32));
	            mutator.insert(c.md5, "Chunk", HFactory.createStringColumn("file_id", c.fileID));
	            mutator.insert(c.md5, "Chunk", HFactory.createStringColumn("index", c.index));
	            mutator.insert(c.md5, "Chunk", HFactory.createStringColumn("length", c.length));
	        } catch (HectorException e) {
	        	log.error("Data was not inserted");
	            e.printStackTrace();
	        }
		}
	}
	
	/**
	 * Retrieves the Column with the key and the column name specified
	 * @param key The line key
	 * @param columnName The column name to get the value
	 * @return The Column with the parameters specified
	 */
	public QueryResult<HColumn<String, String>> getValues(String key, String columnName) {
		 ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
         columnQuery.setColumnFamily("Chunk").setKey(key).setName(columnName);
         QueryResult<HColumn<String, String>> result = columnQuery.execute();
         return result;
	}
	
	/**
	 * Closes the connection with cluster
	 */
	public void close() {
		cluster.getConnectionManager().shutdown();
	}	
}
