package com.dedupeer.thrift;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

public class ThriftServer implements Runnable {
	
	private int port = 7911;
		
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void run() {
		try {
			TServerSocket serverTransport = new TServerSocket(port);

			DeduplicationService.Processor processor = new DeduplicationService.Processor(
					new DeduplicationServiceImpl());
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).

			processor(processor));
			System.out.println("Waiting for Thrift clients on port " + port + " ...");
			server.serve();
			
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}
}
