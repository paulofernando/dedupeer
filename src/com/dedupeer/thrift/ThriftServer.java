package com.dedupeer.thrift;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

public class ThriftServer {
	
	private int port = 7911;
	
	private void start() {
		try {
			TServerSocket serverTransport = new TServerSocket(port);

			DeduplicationService.Processor processor = new DeduplicationService.Processor(
					new DeduplicationServiceImpl());
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).

			processor(processor));
			System.out.println("Starting server on port " + port + " ...");
			server.serve();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ThriftServer srv = new ThriftServer();
		srv.start();

	}
}
