package deduplication.dao.operation;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.log4j.Logger;

import deduplication.dao.DaoUtils;

public class ChunkDaoHector {
	
	private static final Logger log = Logger.getLogger(ChunkDaoHector.class);	
	private Cluster cluster;
	
	private String clusterName, keyspaceName;
	
	public ChunkDaoHector (String clusterName, String keyspaceName) {
		this.clusterName = clusterName;
		this.keyspaceName = keyspaceName;
		cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");
	}
	
	/**
	 * Inserts a new row on the Chunk Column Family
	 */
	public void insertRow(String key, String adler32, String fileID, String index, String length) {		
		try {
            Mutator<String> mutator = HFactory.createMutator(HFactory.createKeyspace(keyspaceName, cluster), StringSerializer.get());
            mutator.insert(key, "Chunk", HFactory.createStringColumn("adler32", adler32));
            mutator.insert(key, "Chunk", HFactory.createStringColumn("file_id", fileID));
            mutator.insert(key, "Chunk", HFactory.createStringColumn("index", index));
            mutator.insert(key, "Chunk", HFactory.createStringColumn("length", length));
        } catch (HectorException e) {
            e.printStackTrace();
        }        
	}
	
	/**
	 * Closes the connection with cluster
	 */
	public void close() {
		cluster.getConnectionManager().shutdown();
	}
	
	public static void main(String[] args) {				
		ChunkDaoHector cdh = new ChunkDaoHector("TestCluster", "Dedupeer");
		cdh.insertRow("ae25d454ff1d414", "45131541631315", "152", "0", "64000");
	}
}
