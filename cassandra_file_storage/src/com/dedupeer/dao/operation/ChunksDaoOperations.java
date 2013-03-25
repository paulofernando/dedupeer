package com.dedupeer.dao.operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.apache.log4j.Logger;

import com.dedupeer.backup.StoredFileFeedback;
import com.dedupeer.thrift.Chunk;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
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
                    Arrays.asList(HFactory.createColumn("content", content)), 
                    stringSerializer, stringSerializer, BytesArraySerializer.get()));          
        } catch (HectorException e) {
        	log.error("Data was not inserted");
            e.printStackTrace();
        }
	}
	
	public void insertRow(Chunk chunk) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
			
			if(chunk.pfile == null) {
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
	                    Arrays.asList(HFactory.createColumn("content", chunk.content.array())), 
	                    stringSerializer, stringSerializer, BytesArraySerializer.get()));
			} else { //Deduplicated chunk
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
	
	/** Inserts a collection of chunks on the Chunk Column Family */
	public void insertRows(ArrayList<Chunk> chunks) {
		int chunk_number = 0;
		for(Chunk c: chunks) {
			try {
				String chunk_num = String.valueOf(chunk_number);
				Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
				
				log.debug("Chunk " + chunk_num + " [length = " + c.length + "]" + " [index = " + c.index + "]" + " [MD5 = " + c.md5 + "]");
				
				if(c.pfile == null) {
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
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList( HFactory.createColumn("content", c.content.array())), 
		                    stringSerializer, stringSerializer, BytesArraySerializer.get()));		            
				} else { //Deduplicated chunk
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
	
	/**
	 * 
	 * @param file_id File ID
	 * @param amountChunks Amount of chunks to retrieve the information
	 * @return HashMap with <adler32, <MD5, chunkNum>>
	 */
	public Map<Integer, Map<String, String>> getHashesOfAFile(String file_id, int amountChunks) {
		SuperSliceQuery<String, String, String, String> query = HFactory.createSuperSliceQuery(keyspaceOperator, stringSerializer, 
				stringSerializer, stringSerializer, stringSerializer);
		query.setColumnFamily("Chunks"); 
		query.setKey(file_id);		
		int loadByTime = 10;		
		Map<Integer, Map<String, String>> chunksLoaded = new HashMap<Integer, Map<String, String>>();		
		long loaded = 0;
		
		while(loaded < amountChunks) {
			//TODO To refactor with the code easier to modify
			if(amountChunks - loaded >= loadByTime) {
				query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2), String.valueOf(loaded + 3), String.valueOf(loaded + 4),
						String.valueOf(loaded + 5), String.valueOf(loaded + 6), String.valueOf(loaded + 7), String.valueOf(loaded + 8), String.valueOf(loaded + 9));
			} else {
				int remains = (int)(amountChunks - loaded);
				switch(remains) {
					case 9:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2), String.valueOf(loaded + 3), 
								String.valueOf(loaded + 4), String.valueOf(loaded + 5), String.valueOf(loaded + 6), String.valueOf(loaded + 7), 
								String.valueOf(loaded + 8));						
						break;
					case 8:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2), String.valueOf(loaded + 3), 
								String.valueOf(loaded + 4), String.valueOf(loaded + 5), String.valueOf(loaded + 6), String.valueOf(loaded + 7));
						break;
					case 7:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2), String.valueOf(loaded + 3), 
								String.valueOf(loaded + 4), String.valueOf(loaded + 5), String.valueOf(loaded + 6));
						break;
					case 6:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2), 
								String.valueOf(loaded + 3), String.valueOf(loaded + 4), String.valueOf(loaded + 5));
						break;
					case 5:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2), String.valueOf(loaded + 3),
								String.valueOf(loaded + 4));
						break;
					case 4:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2), String.valueOf(loaded + 3));
						break;						
					case 3:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1), String.valueOf(loaded + 2));
						break;						
					case 2:
						query.setColumnNames(String.valueOf(loaded), String.valueOf(loaded + 1));
						break;						
					case 1:
						query.setColumnNames(String.valueOf(loaded));
						break;
				}				
			}
			
			QueryResult<SuperSlice<String, String, String>> result = query.execute(); 
						
			for(HSuperColumn<String, String, String> column: result.get().getSuperColumns()) {
				String adler32 = column.getSubColumnByName("adler32").getValue();
				
				if(!chunksLoaded.containsKey(adler32)) {
					Map<String, String> chunkInfo = new HashMap<String, String>();
					chunkInfo.put(column.getSubColumnByName("md5").getValue(),
							String.valueOf(loaded));
					chunksLoaded.put(Integer.parseInt(adler32), chunkInfo);
				} else {
					Map<String, String> md5Set = chunksLoaded.get(adler32);
					md5Set.put(column.getSubColumnByName("md5").getValue(),
							String.valueOf(loaded));
				}
				
				loaded++;
			}			
			log.debug("Last chunk loaded: " + loaded);
		}		
		return chunksLoaded;
	}
	
	public void getAllChunks(String file_id) {
		SliceQuery<String, String, String> query = HFactory.createSliceQuery(keyspaceOperator, StringSerializer.get(),
		    StringSerializer.get(), StringSerializer.get()).
		    setKey(file_id).setColumnFamily("Chunks");

		ColumnSliceIterator<String, String, String> iterator = 
		    new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);
					
		while (iterator.hasNext()) {
		    log.debug(iterator.next().getValue());
		}
	}	
	
	public long getSpaceOccupiedByTheFile(String owner, String filename) {
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
		long bytesStored = 0;
		
		//--------------- retrieving the id ---------------
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//------------------------------------------------
		
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
		
		//--------------- retrieving the id ---------------
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//-------------------------------------------------
		
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
		long i = initialChunk;
		while(result.size() < amountOfChunks) {
	        superColumnQuery.setColumnFamily("Chunks").setKey(fileID).setSuperName(String.valueOf(i));
	        QueryResult<HSuperColumn<String, String, String>> column = superColumnQuery.execute();
	        
	        if(column.get().getSubColumnByName("content") != null) {
	        	result.add(column);
	        }
	        if(feedback != null) {
	        	feedback.updateProgress((int)(Math.ceil((((double)i) * 100) / count)));
	        }
	        i++;
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
		long i = initialChunk;
		while(result.size() < amountOfChunks) {
	        superColumnQuery.setColumnFamily("Chunks").setKey(fileID).setSuperName(String.valueOf(i));
	        QueryResult<HSuperColumn<String, String, String>> column = superColumnQuery.execute();
	        
	        if(column.get().getSubColumnByName("pfile") != null) { 
	        	result.add(column);
	        }
	        if(feedback != null) {
	        	feedback.updateProgress((int)(Math.ceil((((double)i) * 100) / count)));
	        }
	        i++;
		}
        return result;
	}
	
	/** Closes the connection with cluster */
	public void close() {
		cluster.getConnectionManager().shutdown();
	}	
}
