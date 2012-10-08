package deduplication.dao.operation;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import deduplication.dao.Connector;
import deduplication.dao.DaoUtils;

public class ChunkDaoOperations {
	
	private Cassandra.Client client;
	private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
	
	public ChunkDaoOperations(String keyspace) {
		try {
			client = new Connector().connect();
			client.set_keyspace(keyspace);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
	
	public void insertRow(String key, String adler32, String file, String index, String length) {
		long time = System.nanoTime();		
				
        ColumnParent parent = new ColumnParent("chunk");
        ByteBuffer rowid = ByteBuffer.wrap(key.getBytes());
                
        Column adler32Col = new Column();
        adler32Col.setName("adler32".getBytes());
        adler32Col.setValue(adler32.getBytes());
        adler32Col.setTimestamp(time);
        
        Column fileCol = new Column();
        fileCol.setName("file".getBytes());
        fileCol.setValue(file.getBytes());
        fileCol.setTimestamp(time);
        
        Column indexCol = new Column();
        indexCol.setName("index".getBytes());
        indexCol.setValue(index.getBytes());
        indexCol.setTimestamp(time);
        
        Column lengthCol = new Column();
        lengthCol.setName("adler32".getBytes());
        lengthCol.setValue(adler32.getBytes());
        lengthCol.setTimestamp(time);
        
        try {
			client.insert(rowid, parent, adler32Col, consistencyLevel);
			client.insert(rowid, parent, fileCol, consistencyLevel);
			client.insert(rowid, parent, indexCol, consistencyLevel);
			client.insert(rowid, parent, lengthCol, consistencyLevel);			
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		DaoUtils.createSimpleKSandCF("Dedupeer", "chunk", 1);		
		ChunkDaoOperations co = new ChunkDaoOperations("Dedupeer");
		co.insertRow("ade85415ad1a5551e", "45131541631315", "152", "0", "64000");
	}
	
}
