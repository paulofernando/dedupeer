package deduplication.dao.operation;

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

public class UserFilesDaoOperations {
	private static final Logger log = Logger.getLogger(UserFilesDaoOperations.class);	
	private Cluster cluster;
	private static StringSerializer stringSerializer = StringSerializer.get();
	private Keyspace keyspaceOperator;
	
	public UserFilesDaoOperations (String clusterName, String keyspaceName) {
		cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
		keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);
	}
	
	/**
	 * Inserts a new row on the UserFiles SuperColumn Family
	 */
	public void insertRow(String key, String fileName, String fileID, String size, String version) {		
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
            mutator.insert(key, "UserFiles", HFactory.createSuperColumn(fileName, 
                    Arrays.asList(HFactory.createStringColumn("file_id", fileID)), 
                    stringSerializer, stringSerializer, stringSerializer));
            mutator.insert(key, "UserFiles", HFactory.createSuperColumn(fileName, 
                    Arrays.asList(HFactory.createStringColumn("size", size)), 
                    stringSerializer, stringSerializer, stringSerializer));
            mutator.insert(key, "UserFiles", HFactory.createSuperColumn(fileName, 
                    Arrays.asList(HFactory.createStringColumn("version", version)), 
                    stringSerializer, stringSerializer, stringSerializer));
            
        } catch (HectorException e) {
            e.printStackTrace();
        }        
	}
	
	public QueryResult<HSuperColumn<String, String, String>> getValues(String key, String fileName) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
                    stringSerializer, stringSerializer);
		
        superColumnQuery.setColumnFamily("UserFiles").setKey(key).setSuperName(fileName);
        QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();

        System.out.println("Read HSuperColumn from cassandra: " + result.get());        
        return result;
	}
	
	/**
	 * Closes the connection with cluster
	 */
	public void close() {
		cluster.getConnectionManager().shutdown();
	}
	
	public static void main(String[] args) {				
		UserFilesDaoOperations cdh = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		cdh.insertRow("paulofernando", "lorem.txt", "123", "128000", "2");
	}
}
