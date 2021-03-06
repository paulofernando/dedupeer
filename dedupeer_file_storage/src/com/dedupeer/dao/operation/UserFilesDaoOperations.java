package com.dedupeer.dao.operation;

import java.util.Arrays;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperColumnQuery;

import org.apache.log4j.Logger;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class UserFilesDaoOperations {

	private static final Logger log = Logger.getLogger(UserFilesDaoOperations.class);

	private Cluster cluster;
	private Keyspace keyspaceOperator;
	private static StringSerializer stringSerializer = StringSerializer.get();
	
	/**
	 * Creates an object to manipulate the operations on the UserFiles SuperColumn Family 
	 * @param clusterName The cluster name from instance of Cassandra
	 * @param keyspaceName The Keyspace name where the UserFiles SuperColumn Family was created
	 */
	public UserFilesDaoOperations(String clusterName, String keyspaceName) {
		cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
		keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);
	}

	/** Inserts a new row on the UserFiles SuperColumn Family */
	@SuppressWarnings("unchecked")
	public void insertRow(String owner_name, String fileName, String fileID, String size, String chunks, String version, int defaultChunkSize) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
			mutator.insert(owner_name, "UserFiles", HFactory.createSuperColumn(
					fileName, Arrays.asList(HFactory.createStringColumn("file_id", fileID)), stringSerializer, stringSerializer, stringSerializer));
			mutator.insert(owner_name, "UserFiles", HFactory.createSuperColumn(
					fileName, Arrays.asList(HFactory.createStringColumn("size", size)), stringSerializer, stringSerializer, stringSerializer));
			mutator.insert(owner_name, "UserFiles", HFactory.createSuperColumn(
					fileName, Arrays.asList(HFactory.createStringColumn("chunks", chunks)), stringSerializer, stringSerializer, stringSerializer));
			mutator.insert(owner_name, "UserFiles", HFactory.createSuperColumn(
					fileName, Arrays.asList(HFactory.createStringColumn("version", version)), stringSerializer, stringSerializer, stringSerializer));
			mutator.insert(owner_name, "UserFiles", HFactory.createSuperColumn(
					fileName, Arrays.asList(HFactory.createStringColumn("default_chunk_size", String.valueOf(defaultChunkSize))), stringSerializer, stringSerializer, stringSerializer));
		} catch (HectorException e) {
			log.error("Data was not inserted");
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public void setAmountChunksWithContent(String owner_name, String fileName, long amountChunks) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
			mutator.insert(owner_name, "UserFiles", HFactory.createSuperColumn(
					fileName, Arrays.asList(HFactory.createStringColumn("chunks_with_content", String.valueOf(amountChunks))), stringSerializer, stringSerializer, stringSerializer));			
		} catch (HectorException e) {
			log.error("Data was not inserted");
			e.printStackTrace();
		}		
	}
	
	@SuppressWarnings("unchecked")
	public void setAmountChunksWithoutContent(String owner_name, String fileName, long amountChunks) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
			mutator.insert(owner_name, "UserFiles", HFactory.createSuperColumn(
					fileName, Arrays.asList(HFactory.createStringColumn("chunks_without_content", String.valueOf(amountChunks))), stringSerializer, stringSerializer, stringSerializer));			
		} catch (HectorException e) {
			log.error("Data was not inserted");
			e.printStackTrace();
		}		
	}

	/**
	 * Retrieves the Super Column with the key and the column specified 
	 * @param owner_name The line key (the owner name of file)
	 * @param fileName The filename that is a column name of the SuperColumn
	 * @return The SuperColumns with the parametres specified
	 */
	public QueryResult<HSuperColumn<String, String, String>> getValues(String owner_name, String fileName) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = HFactory
				.createSuperColumnQuery(keyspaceOperator, stringSerializer,
						stringSerializer, stringSerializer, stringSerializer);

		superColumnQuery.setColumnFamily("UserFiles").setKey(owner_name).setSuperName(fileName);
		QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();
		return result;
	}
	
	public boolean fileExists(String owner, String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		QueryResult<HSuperColumn<String, String, String>> result = ufdo.getValues(owner, filename);
		return (result.get() != null);
	}
	
	/**
	 * Retrieves the amount of chunk of a file 
	 * @param owner File's owner
	 * @param filename File name
	 * @return
	 */
	public long getChunksCount(String owner, String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		QueryResult<HSuperColumn<String, String, String>> userFileResult = ufdo.getValues(owner, filename);
		HColumn<String, String> columnAmountChunks = userFileResult.get().getSubColumnByName("chunks");
		return Long.parseLong(columnAmountChunks.getValue());
	}
	
	/**
	 * Retrieves the amount of chunk with content of a file
	 * @param owner File's owner
	 * @param filename File name
	 * @return The amount of chunks with content 
	 */
	public long getChunksWithContentCount(String owner, String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		QueryResult<HSuperColumn<String, String, String>> userFileResult = ufdo.getValues(owner, filename);
		HColumn<String, String> columnAmountChunks = userFileResult.get().getSubColumnByName("chunks_with_content");
		return Long.parseLong(columnAmountChunks.getValue());
	}
	
	/**
	 * Retrieves the amount of references to others chunks of a file.  These are the amount of deduplicated chunks
	 * @param owner File's owner
	 * @param filename File name
	 * @return The amount of chunks without content, i.e. references to others chunks 
	 */
	public long getChunksWithoutContentCount(String owner, String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		QueryResult<HSuperColumn<String, String, String>> userFileResult = ufdo.getValues(owner, filename);
		HColumn<String, String> columnAmountChunks = userFileResult.get().getSubColumnByName("chunks_without_content");
		return Long.parseLong(columnAmountChunks.getValue());
	}
	
	/**
	 * Retrieves the size of file 
	 * @param owner File's owner
	 * @param filename File name
	 * @return
	 */
	public long getFileLength(String owner, String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		QueryResult<HSuperColumn<String, String, String>> userFileResult = ufdo.getValues(owner, filename);
		HColumn<String, String> columnAmountChunks = userFileResult.get().getSubColumnByName("size");
		return Long.parseLong(columnAmountChunks.getValue());
	}
	
	/**
	 * Retrieves the chunk size used when the file was stored. 
	 * @param owner File's owner
	 * @param filename File name
	 * @return
	 */
	public int getDefaultChunkSize(String owner, String filename) {
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		QueryResult<HSuperColumn<String, String, String>> userFileResult = ufdo.getValues(owner, filename);
		HColumn<String, String> columnAmountChunks = userFileResult.get().getSubColumnByName("default_chunk_size");
		return Integer.parseInt(columnAmountChunks.getValue());
	}

	/** Closes the connection with cluster */
	public void close() {
		cluster.getConnectionManager().shutdown();
	}
}
