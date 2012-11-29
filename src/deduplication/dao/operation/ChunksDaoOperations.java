package deduplication.dao.operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;

import org.apache.log4j.Logger;

import deduplication.backup.StoredFileFeedback;
import deduplication.dao.ChunksDao;
import deduplication.utils.FileUtils;

public class ChunksDaoOperations {
	
	private static final Logger log = Logger.getLogger(ChunksDaoOperations.class);
	
	private Cluster cluster;	
	private Keyspace keyspaceOperator;
	
	private static StringSerializer stringSerializer = StringSerializer.get();
	private StoredFileFeedback feedback;
	
	/**
	 * Creates an object to manipulate the operations on the Chunks Column Family
	 * @param clusterName The cluster name from instance of Cassandra
	 * @param keyspaceName The Keyspace name where the Chunks Column Family was created
	 */
	public ChunksDaoOperations (String clusterName, String keyspaceName) {
		cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");
		keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);
		
	}
	
	/**
	 * Creates an object to manipulate the operations on the Chunks Column Family
	 * @param clusterName The cluster name from instance of Cassandra
	 * @param keyspaceName The Keyspace name where the Chunks Column Family was created 
	 * @param StoredFileFeedback to inform the current progress
	 */
	public ChunksDaoOperations (String clusterName, String keyspaceName, StoredFileFeedback feedback) {
		this(clusterName, keyspaceName);
		this.feedback = feedback;
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
	
	public void insertRow(ChunksDao chunk) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
			
			if(chunk.pfile.equals("")) {
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("md5", chunk.md5)), 
	                    stringSerializer, stringSerializer, stringSerializer));
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("adler32", chunk.adler32)), 
	                    stringSerializer, stringSerializer, stringSerializer));
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("index", chunk.index)), 
	                    stringSerializer, stringSerializer, stringSerializer));
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("length", chunk.length)), 
	                    stringSerializer, stringSerializer, stringSerializer));
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createColumn("content", chunk.content)), 
	                    stringSerializer, stringSerializer, BytesArraySerializer.get()));
			} else { //deduplicated chunk
				mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("index", chunk.index)), 
	                    stringSerializer, stringSerializer, stringSerializer));
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("pfile", chunk.pfile)), 
	                    stringSerializer, stringSerializer, stringSerializer));
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("pchunk", chunk.pchunk)), 
	                    stringSerializer, stringSerializer, stringSerializer));
			}
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
				
				log.info("Chunk " + chunk_num + " [adler32 = " + c.adler32 + "] e [MD5 = " + c.md5 + "]");
				
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
			                    Arrays.asList( HFactory.createColumn("content", FileUtils.getBytesFromFile(c.destination))), 
			                    stringSerializer, stringSerializer, BytesArraySerializer.get()));
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
				}
	            chunk_number++;
	            
	            if(feedback != null) {
	            	feedback.updateProgress((int) Math.ceil((((double)chunk_number) * 100) / chunks.size()));
	            }
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
	 * @return The SuperColumn with the parameters specified
	 */
	public QueryResult<HSuperColumn<String, String, String>> getValues(String file_id, String chunk_number) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
			
        superColumnQuery.setColumnFamily("Chunks").setKey(file_id).setSuperName(chunk_number);
        QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();    
        return result;
	}
	
	public void getAllChunks(String file_id) {
		SliceQuery<String, String, String> query = HFactory.createSliceQuery(keyspaceOperator, StringSerializer.get(),
			    StringSerializer.get(), StringSerializer.get()).
			    setKey(file_id).setColumnFamily("Chunks");

			ColumnSliceIterator<String, String, String> iterator = 
			    new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);
						
			while (iterator.hasNext()) {
			    System.out.println(iterator.next().getValue());
			}
	}
	
	
	public long getSpaceOccupiedByTheFile(String owner, String filename) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
		long bytesStored = 0;
		
		//--- retrieving the id ----
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//-------------------------
		
		long count = ufdo.getChunksCount(owner, filename);	
		for(int i = 0; i < count; i++) {
	        superColumnQuery.setColumnFamily("Chunks").setKey(fileID).setSuperName(String.valueOf(i));
	        QueryResult<HSuperColumn<String, String, String>> column = superColumnQuery.execute();
	        
	        if(column.get().getSubColumnByName("content") != null) {
				bytesStored += Integer.parseInt(column.get().getSubColumnByName("length").getValue());
	        }
	        
	        if(feedback != null) {
            	feedback.updateProgress((int) Math.ceil((((double)i) * 100) / count));
            }
		}
        return bytesStored;
	}
	
	public Vector<QueryResult<HSuperColumn<String, String, String>>> getAllValuesWithContent(String owner, String filename) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
			
		Vector<QueryResult<HSuperColumn<String, String, String>>> result = new Vector<QueryResult<HSuperColumn<String, String, String>>>();
		
		//--- retrieving the id ----
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//-------------------------
		
		long count = ufdo.getChunksCount(owner, filename);		
		for(int i = 0; i < count; i++) {
	        superColumnQuery.setColumnFamily("Chunks").setKey(fileID).setSuperName(String.valueOf(i));
	        QueryResult<HSuperColumn<String, String, String>> column = superColumnQuery.execute();
	        
	        if(column.get().getSubColumnByName("content") != null) {
	        	result.add(column);
	        }
	        if(feedback != null) {
	        	feedback.updateProgress((int)(Math.ceil((((double)i) * 100) / count)));
	        }
		}
        return result;
	}
	
	public Vector<QueryResult<HSuperColumn<String, String, String>>> getValuesWithContent(String owner, String filename, long initialChunk, long amountOfChunks) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
			
		Vector<QueryResult<HSuperColumn<String, String, String>>> result = new Vector<QueryResult<HSuperColumn<String, String, String>>>();
		
		//--- retrieving the id ----
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//-------------------------
		
		long count = ufdo.getChunksCount(owner, filename);
		long finalChunk = initialChunk + amountOfChunks;
		for(long i = initialChunk; i < finalChunk; i++) {
	        superColumnQuery.setColumnFamily("Chunks").setKey(fileID).setSuperName(String.valueOf(i));
	        QueryResult<HSuperColumn<String, String, String>> column = superColumnQuery.execute();
	        
	        if(column.get().getSubColumnByName("content") != null) {
	        	result.add(column);
	        }
	        if(feedback != null) {
	        	feedback.updateProgress((int)(Math.ceil((((double)i) * 100) / count)));
	        }
		}
        return result;
	}
	
	/**
	 * Retrieves all columns with content in bytes of file
	 * @param file_id The line key
	 * @param chunk_number The column name to get the value
	 * @return The SuperColumns with the column content
	 */
	public Vector<QueryResult<HSuperColumn<String, String, String>>> getValuesWithoutContent(String owner, String filename, long initialChunk, long amountOfChunks) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
			
		Vector<QueryResult<HSuperColumn<String, String, String>>> result = new Vector<QueryResult<HSuperColumn<String, String, String>>>();
		
		//--- retrieving the id ----
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//-------------------------
		
		long count = ufdo.getChunksCount(owner, filename);
		long finalChunk = initialChunk + amountOfChunks;
		for(long i = initialChunk; i < finalChunk; i++) {
	        superColumnQuery.setColumnFamily("Chunks").setKey(fileID).setSuperName(String.valueOf(i));
	        QueryResult<HSuperColumn<String, String, String>> column = superColumnQuery.execute();
	        
	        if(column.get().getSubColumnByName("pfile") != null) { 
	        	result.add(column);
	        }
	        if(feedback != null) {
	        	feedback.updateProgress((int)(Math.ceil((((double)i) * 100) / count)));
	        }
		}
        return result;
	}
	
	/**
	 * Closes the connection with cluster
	 */
	public void close() {
		cluster.getConnectionManager().shutdown();
	}	
}
