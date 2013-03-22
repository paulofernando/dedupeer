package com.dedupeer.thrift;

import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftClient {
	
	private static ThriftClient thriftClient;
	
	public static ThriftClient getInstance() {
		if(thriftClient == null) {
			thriftClient = new ThriftClient();
		}
		return thriftClient;
	}
	
	public void deduplicate(Map<Integer,Map<String,String>> chunksInfo, String pathOfFile, int chunkSizeInBytes, int bytesToLoadByTime) {

		TTransport transport;

		try {
			transport = new TSocket("localhost", 7911);
			TProtocol protocol = new TBinaryProtocol(transport);
			DeduplicationService.Client client = new DeduplicationService.Client(protocol);

			transport.open();
			Map<Long,Chunk> chunks = client.deduplicate(chunksInfo, pathOfFile, chunkSizeInBytes, bytesToLoadByTime);
			System.out.println("Chunks size: " + chunks.size());
			
			//long multiplyResult = client.multiply(20, 40);
			//System.out.println("Multiply result: " + multiplyResult);
			transport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
