package deduplication.dao.operation;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class InsertSingleColumn {
	 private static StringSerializer stringSerializer = StringSerializer.get();
	    
    public static void main(String[] args) throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        Keyspace keyspaceOperator = HFactory.createKeyspace("testkeyspace", cluster);
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
            mutator.insert("jsmith", "Standard1", HFactory.createStringColumn("first", "John"));
            
            ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
            columnQuery.setColumnFamily("Standard1").setKey("jsmith").setName("first");
            QueryResult<HColumn<String, String>> result = columnQuery.execute();
            
            System.out.println("Read HColumn from cassandra: " + result.get());            
            System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");
            
        } catch (HectorException e) {
            e.printStackTrace();
        }
        cluster.getConnectionManager().shutdown();
    }
}
