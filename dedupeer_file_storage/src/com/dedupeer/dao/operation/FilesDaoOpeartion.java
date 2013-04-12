package com.dedupeer.dao.operation;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class FilesDaoOpeartion {

	private Cluster cluster;
	private Keyspace keyspaceOperator;
	
	/**
	 * Creates an object to manipulate the operations on the Files Column Family 
	 * @param clusterName The cluster name from instance of Cassandra
	 * @param keyspaceName The Keyspace name where the File Column Family was created
	 */
	public FilesDaoOpeartion(String clusterName, String keyspaceName) {
		cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");
		keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);
	}
	
	/** Inserts a new row on the Files Column Family */
	public void insertRow(String ownerName, String fileName, String fileID) {
		Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
        mutator.insert(ownerName, "Files", HFactory.createStringColumn(fileName, fileID));
	}
	
	/**
	 * Retrieves the files information of a owner
	 * @param ownerName Owner of the files
	 * @return A Map of file information. Filename as key and fileID as value
	 */
	public Map<String, Long> getAllFiles(String ownerName) {
		SliceQuery<String,String,String> query = HFactory.createSliceQuery(keyspaceOperator, 
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
	    query.setColumnFamily("Files").setKey(ownerName).setRange(null, null, false, 100);
	    
	    QueryResult<ColumnSlice<String,String>> result = query.execute();
	    Map<String, Long> files = new HashMap<String, Long>();
	    for (HColumn<String, String> column : result.get().getColumns()) {
	        files.put(column.getName(), Long.parseLong(column.getValue()));
	    }
		return files;
	}
	
	/**
	 * Retrieves a ID of a file
	 * @param ownerName File's owner
	 * @param filename File name
	 * @return ID of the file
	 */
	public String getFileID(String ownerName, String filename) {
		ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
        columnQuery.setColumnFamily("Files").setKey(ownerName).setName(filename);
        QueryResult<HColumn<String, String>> result = columnQuery.execute();
        return result.get().getValue();
	}	
}
