package com.dedupeer.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftClient {
	
	private void invoke() {

		TTransport transport;

		try {
			transport = new TSocket("localhost", 7911);
			TProtocol protocol = new TBinaryProtocol(transport);
			DeduplicationService.Client client = new DeduplicationService.Client(protocol);

			transport.open();
			//long addResult = client.deduplicate(chunksInfo, pathOfFile, chunkSizeInBytes, bytesToLoadByTime)
			//System.out.println("Add result: " + addResult);
			//long multiplyResult = client.multiply(20, 40);
			//System.out.println("Multiply result: " + multiplyResult);
			transport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ThriftClient c = new ThriftClient();
		c.invoke();
	}
}
