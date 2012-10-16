package deduplication.dao.operation;

import java.util.ArrayList;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperColumnQuery;

import org.apache.log4j.Logger;

import deduplication.dao.ChunksDao;
import deduplication.utils.FileUtils;

public class ChunksDaoOperations {
	
	private static final Logger log = Logger.getLogger(ChunksDaoOperations.class);
	
	private Cluster cluster;	
	private Keyspace keyspaceOperator;
	
	private static StringSerializer stringSerializer = StringSerializer.get();
	
	/**
	 * Creates an object to manipulate the operations on the Chunks Column Family
	 * @param clusterName The cluster name from instance of Cassandra
	 * @param keyspaceName The Keyspace name where the Chunks Column Family was created 
	 */
	public ChunksDaoOperations (String clusterName, String keyspaceName) {
		cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");
		keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);
	}
		
	public void insertRow(String fileID, String chunk_num, String md5, String adler32, String index, String length, byte[] content) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
            mutator.insert(fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
                    Arrays.asList(HFactory.createStringColumn("md5", md5)), 
                    stringSerializer, stringSerializer, stringSerializer));
            mutator.insert(fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
                    Arrays.asList(HFactory.createStringColumn("adler32", adler32)), 
                    stringSerializer, stringSerializer, stringSerializer));
            mutator.insert(fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
                    Arrays.asList(HFactory.createStringColumn("index", index)), 
                    stringSerializer, stringSerializer, stringSerializer));
            mutator.insert(fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
                    Arrays.asList(HFactory.createStringColumn("length", length)), 
                    stringSerializer, stringSerializer, stringSerializer));
            mutator.insert(fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
                    Arrays.asList(HFactory.createColumn("content", new String(content))), 
                    stringSerializer, stringSerializer, stringSerializer));          
        } catch (HectorException e) {
        	log.error("Data was not inserted");
            e.printStackTrace();
        }
	}
	
	/**
	 * Inserts a collection of chunks on the Chunk Column Family
	 */
	public void insertRows(ArrayList<ChunksDao> chunks) {
		int chunk_number = 0;
		for(ChunksDao c: chunks) {
			try {
				String chunk_num = String.valueOf(chunk_number);
				Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
				
				if(c.pfile.equals("")) {
					mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("md5", c.md5)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("adler32", c.adler32)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("index", c.index)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("length", c.length)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            if(!c.destination.equals("")) {
		            	mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
			                    Arrays.asList( HFactory.createColumn("content", new String(FileUtils.getBytesFromFile(c.destination)))), 
			                    stringSerializer, stringSerializer, stringSerializer));
		            }
				} else { //deduplicated chunk
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("index", c.index)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("pfile", c.pfile)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("pchunk", c.pchunk)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            if(!c.destination.equals("")) {
		            	mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
			                    Arrays.asList(HFactory.createColumn("content", new String(FileUtils.getBytesFromFile(c.destination)))), 
			                    stringSerializer, stringSerializer, stringSerializer));
		            }
				}
	            chunk_number++;

	        } catch (HectorException e) {
	        	log.error("Data was not inserted");
	            e.printStackTrace();
	        }
		}
	}
	
	/**
	 * Retrieves the Column with the key and the column name specified
	 * @param file_id The line key
	 * @param columnName The column name to get the value
	 * @return The Column with the parameters specified
	 */
	public QueryResult<HSuperColumn<String, String, String>> getValues(String file_id, String chunk_number) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
			
        superColumnQuery.setColumnFamily("Chunks").setKey(file_id).setSuperName(chunk_number);
        QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();    
        return result;
	}
	
	/**
	 * Closes the connection with cluster
	 */
	public void close() {
		cluster.getConnectionManager().shutdown();
	}	
}
