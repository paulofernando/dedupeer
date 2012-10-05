package deduplication.dao;

import static deduplication.dao.Constants.KEYSPACE;
import static deduplication.dao.Constants.HOST;
import static deduplication.dao.Constants.PORT;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import deduplication.processing.file.Chunking;

/**
 * Starts a connection to Cassandra 
 * @author Paulo Fernando
 *
 */
public class Connector {
	
	private static final Logger log = Logger.getLogger(Connector.class);
	
	private TTransport tt = new TSocket(HOST, PORT);
	
	/**
	 * Connects to Cassandra with params sets up in {@link Constants}
	 * @return Cassandra client
	 * @throws InvalidRequestException
	 * @throws TException
	 */
	public Cassandra.Client connect() throws InvalidRequestException, TException {
		TFramedTransport tf = new TFramedTransport(tt);
		TProtocol protocol = new TBinaryProtocol(tf);
		Cassandra.Client cassandraClient = new Cassandra.Client(protocol);
		
		tt.open();
		cassandraClient.set_keyspace(KEYSPACE);
		
		return cassandraClient;
	}
	
	/**
	 * Closes a connection with Cassandra
	 */
	public void close() {
		try {
			tt.flush();
		} catch (TTransportException e) {
			log.error(e);
		}
		tt.close();
	}
	
}
