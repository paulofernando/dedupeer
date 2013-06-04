package com.dedupeer.dao.operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.KeyspaceService;
import me.prettyprint.cassandra.service.KeyspaceServiceImpl;
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
import com.dedupeer.thrift.ChunkIDs;
import com.dedupeer.utils.Range;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class ChunksDaoOperations {
	
	private static final Logger log = Logger.getLogger(ChunksDaoOperations.class);
	
	private Cluster cluster;	
	private Keyspace keyspaceOperator;
	
	private static StringSerializer stringSerializer = StringSerializer.get();
	private StoredFileFeedback feedback;
	
	KeyspaceService keyspace;
	
	/**
	 * Creates an object to manipulate the operations on the Chunks Column Family
	 * @param clusterName The cluster name from instance of Cassandra
	 * @param keyspaceName The Keyspace name where the Chunks Column Family was created
	 */
	public ChunksDaoOperations (String clusterName, String keyspaceName) {
		cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");
		keyspaceOperator = HFactory.createKeyspace(keyspaceName, cluster);		
		
		HConnectionManager connectionManager = new HConnectionManager(clusterName, new CassandraHostConfigurator("localhost:9160"));
		
		keyspace = new KeyspaceServiceImpl(keyspaceName, new QuorumAllConsistencyLevelPolicy(), 
				connectionManager, FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE);
		
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
		
	@SuppressWarnings("unchecked")
	public void insertRow(String fileID, String chunk_num, String strongHash, String weakHash, String index, String length, byte[] content) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
            mutator.insert(fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
                    Arrays.asList(HFactory.createStringColumn("strongHash", strongHash)), 
                    stringSerializer, stringSerializer, stringSerializer));
            mutator.insert(fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
                    Arrays.asList(HFactory.createStringColumn("weakHash", weakHash)), 
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
	
	@SuppressWarnings("unchecked")
	public void insertRow(Chunk chunk) {
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
			
			if(chunk.pfile == null) {
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("strongHash", chunk.strongHash)), 
	                    stringSerializer, stringSerializer, stringSerializer));
	            mutator.insert(chunk.fileID, "Chunks", HFactory.createSuperColumn(chunk.chunkNumber, 
	                    Arrays.asList(HFactory.createStringColumn("weakHash", chunk.weakHash)), 
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
	@SuppressWarnings("unchecked")
	public void insertRows(ArrayList<Chunk> chunks, int initialChunk) {
		int chunk_number = initialChunk;
		for(Chunk c: chunks) {
			try {
				String chunk_num = String.valueOf(chunk_number);
				Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
				
				log.info("Chunk " + chunk_num + " [length = " + c.length + "]" + " [index = " + c.index + "]" + " [StrongHash = " + c.strongHash + "]");
				
				if(c.pfile == null) {
					mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("strongHash", c.strongHash)), 
		                    stringSerializer, stringSerializer, stringSerializer));
		            mutator.insert(c.fileID, "Chunks", HFactory.createSuperColumn(chunk_num, 
		                    Arrays.asList(HFactory.createStringColumn("weakHash", c.weakHash)), 
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
	 * @return HashMap with <weakHash, <strongHash, chunkNum>>
	 */
	public Map<Integer, Map<String, ChunkIDs>> getHashesOfAFile(String file_id, int amountChunks) {
		SuperSliceQuery<String, String, String, String> query = HFactory.createSuperSliceQuery(keyspaceOperator, stringSerializer, 
				stringSerializer, stringSerializer, stringSerializer);
		query.setColumnFamily("Chunks"); 
		query.setKey(file_id);		
		int loadByTime = 10;		
		Map<Integer, Map<String, ChunkIDs>> chunksLoaded = new HashMap<Integer, Map<String, ChunkIDs>>();		
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
				String weakHash = column.getSubColumnByName("weakHash").getValue();
				
				if(!chunksLoaded.containsKey(weakHash)) {
					Map<String, ChunkIDs> chunkInfo = new HashMap<String, ChunkIDs>();
					ChunkIDs ids = new ChunkIDs();
					ids.setChunkID(String.valueOf(loaded));
					ids.setFileID(file_id);
					chunkInfo.put(column.getSubColumnByName("strongHash").getValue(), ids);
					
					chunksLoaded.put(Integer.parseInt(weakHash), chunkInfo);
				} else {
					Map<String, ChunkIDs> strongHashSet = chunksLoaded.get(weakHash);
					ChunkIDs ids = new ChunkIDs();
					ids.setChunkID(String.valueOf(loaded));
					ids.setFileID(file_id);
					strongHashSet.put(column.getSubColumnByName("strongHash").getValue(), ids);
				}
				
				loaded++;
			}			
			log.info("Last chunk loaded: " + loaded);
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
		//--------------- retrieving the id ---------------
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//------------------------------------------------
		
		SuperSliceQuery<String, String, String, String> query = HFactory.createSuperSliceQuery(keyspaceOperator, stringSerializer, 
				stringSerializer, stringSerializer, stringSerializer);
		query.setColumnFamily("Chunks"); 
		query.setKey(fileID);
		long chunksLoaded = 0, bytesStored = 0, i = 0, count = ufdo.getChunksCount(owner, filename);
		QueryResult<SuperSlice<String, String, String>> result;
		
		do {
			query.setColumnNames(String.valueOf(i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //5
					String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //10
					String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //15
					String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i)); //20					
			result = query.execute(); 
			
			for(HSuperColumn<String, String, String> chunk: result.get().getSuperColumns()) {	        
		        if(chunk.getSubColumnByName("content") != null) { //chunk with content
		        	bytesStored += Integer.parseInt(chunk.getSubColumnByName("length").getValue());
		        }		        
		        if(feedback != null) feedback.updateProgress((int) Math.ceil((((double)i) * 100) / count));	            
		        chunksLoaded++;
			}
			
			log.info("Calculating economy of " + filename + ": " + chunksLoaded);
		} while(result.get().getSuperColumns().size() != 0);
		
        return bytesStored;
	}
	
	public List<Range> getAreasModified(String owner, String filename) {
		//--------------- retrieving the id ---------------
		UserFilesDaoOperations ufdo = new UserFilesDaoOperations("TestCluster", "Dedupeer");
		HColumn<String, String> columnFileID = ufdo.getValues(owner, filename).get().getSubColumnByName("file_id");
		String fileID = columnFileID.getValue();
		//------------------------------------------------
		
		ArrayList<Range> chunksPosition = new ArrayList<Range>();
		
		SuperSliceQuery<String, String, String, String> query = HFactory.createSuperSliceQuery(keyspaceOperator, stringSerializer, 
				stringSerializer, stringSerializer, stringSerializer);
		query.setColumnFamily("Chunks"); 
		query.setKey(fileID);
		
		long i = 0, chunksLoaded = 0, index = 0, count = ufdo.getChunksCount(owner, filename);
		int percent = 0;
		QueryResult<SuperSlice<String, String, String>> result;
		
		do {
			query.setColumnNames(String.valueOf(i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //5
					String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //10
					String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //15
					String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i)); //20					
			result = query.execute(); 
			
			for(HSuperColumn<String, String, String> chunk: result.get().getSuperColumns()) {	        
		        if(chunk.getSubColumnByName("length") != null) { //chunk with content
		        	index = Long.parseLong(chunk.getSubColumnByName("index").getValue()); 
		        	chunksPosition.add(new Range(index, index + Long.parseLong(chunk.getSubColumnByName("length").getValue())));
		        }
		        chunksLoaded++;
			}
			
			if(feedback != null) {
	        	percent = (int) Math.ceil((((double)chunksLoaded) * 100) / count);
	        	feedback.updateProgress(percent);
	        }
			
			log.info("Analyzing " + filename + ": " + chunksLoaded);
		} while(result.get().getSuperColumns().size() != 0);
		
        return mergeAreas(chunksPosition);
	}
	
	/**
	 * Join the areas that have the final index equals with the initial index of the chunks. 
	 * For instance, the ranges (0,10) and (10,14) turns into (0,14). 
	 * @param ranges Chunks position to merge
	 * @return Merged areas
	 */
	private static List<Range> mergeAreas(List<Range> ranges) {
		//TODO Change to Interval tree to be more fast
		
		List<Range> result = new ArrayList<Range>();
		if((ranges != null) && (ranges.size() > 0)) {
			Collections.sort(ranges);			
						
			result.add(new Range(ranges.get(0).getInitialValue(), ranges.get(0).getFinalValue()));
			for(int i = 1; i < (ranges.size()); i++) {
				if(result.get(result.size()-1).getFinalValue() == ranges.get(i).getInitialValue()) {
					result.get(result.size()-1).setFinalValue(ranges.get(i).getFinalValue());
				} else {
					result.add(new Range(ranges.get(i).getInitialValue(), ranges.get(i).getFinalValue()));
				}
			}
		}		
		return result;		
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
	
	/**
	 * Retrieves the range of super columns (chunks). Chunks with and without content are included
	 */
	public Vector<QueryResult<HSuperColumn<String, String, String>>> getValuesWithContent(String owner, String filename, String fileID, 
			long chunksCount,long initialChunk, long amountOfChunks) {
		
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
	            HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
	                    stringSerializer, stringSerializer);
			
		Vector<QueryResult<HSuperColumn<String, String, String>>> result = new Vector<QueryResult<HSuperColumn<String, String, String>>>();
				
		long i = initialChunk;
		while(result.size() < amountOfChunks) {
	        superColumnQuery.setColumnFamily("Chunks").setKey(fileID).setSuperName(String.valueOf(i));
	        QueryResult<HSuperColumn<String, String, String>> column = superColumnQuery.execute();
	        
	        if(column.get().getSubColumnByName("content") != null) {
	        	result.add(column);
	        }
	        
	        i++;
		}
        return result;
	}
	
	
	public QueryResult<SuperSlice<String, String, String>> getChunksByRange(String owner, String filename, String fileID, 
			long chunksCount,long initialChunk) {
				
		SuperSliceQuery<String, String, String, String> query = HFactory.createSuperSliceQuery(keyspaceOperator, stringSerializer, 
				stringSerializer, stringSerializer, stringSerializer);
		query.setColumnFamily("Chunks"); 
		query.setKey(fileID);
		
		long i = initialChunk;
		query.setColumnNames(String.valueOf(i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //5
				String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //10
				String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), //15
				String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i), String.valueOf(++i)); //20
				
		QueryResult<SuperSlice<String, String, String>> result = query.execute(); 
		
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
