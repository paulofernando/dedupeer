package com.dedupeer.dao;

import java.util.ArrayList;
import java.util.Arrays;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraManager {
	
	private static final String CLUSTER_NAME = "TestCluster";
	private static final String KEYSPACE_NAME = "Dedupeer";
	private static final String HOST = "localhost:9160";
	private int replicationFactor = 1;
	
	public void createDedupeerDataModel() {
		Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME,HOST);
		
		ColumnFamilyDefinition cfFiles = HFactory.createColumnFamilyDefinition(KEYSPACE_NAME,                              
                "Files", 
                ComparatorType.UTF8TYPE);		
		
	    ThriftCfDef cfUserFiles = (ThriftCfDef)HFactory.createColumnFamilyDefinition(KEYSPACE_NAME, "UserFiles",  ComparatorType.UTF8TYPE, new ArrayList<ColumnDefinition>() );
	    	cfUserFiles.setColumnType(ColumnType.SUPER);
	    	cfUserFiles.setSubComparatorType(ComparatorType.UTF8TYPE);
		
    	ThriftCfDef cfChunks = (ThriftCfDef)HFactory.createColumnFamilyDefinition(KEYSPACE_NAME, "Chunks",  ComparatorType.BYTESTYPE, new ArrayList<ColumnDefinition>() );
    		cfChunks.setColumnType(ColumnType.SUPER);
    		cfChunks.setSubComparatorType(ComparatorType.BYTESTYPE);
    		cfChunks.setComparatorType(ComparatorType.BYTESTYPE);
    		cfChunks.setDefaultValidationClass("BytesType");
    		cfChunks.setKeyValidationClass("BytesType");	    	
	    	
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(KEYSPACE_NAME,                 
                ThriftKsDef.DEF_STRATEGY_CLASS,  
                replicationFactor, 
                Arrays.asList(cfFiles));
		
		cluster.addKeyspace(newKeyspace, true);
		cluster.addColumnFamily(cfUserFiles);
		cluster.addColumnFamily(cfChunks);
	}
	
	/**
	 * @return if the keyspace was dropped or not.
	 */
	public boolean dropDedupeerDataModel() {
		CassandraHostConfigurator  cassandraHostConfigurator = new CassandraHostConfigurator(HOST);
		ThriftCluster cassandraCluster = new ThriftCluster(CLUSTER_NAME, cassandraHostConfigurator);	    
		String ks = cassandraCluster.dropKeyspace(KEYSPACE_NAME, true);	    
	    return (ks != null);
	}
	
	public boolean isDedupeerKeySpaceCreated() {
		CassandraHostConfigurator  cassandraHostConfigurator = new CassandraHostConfigurator(HOST);
		ThriftCluster cassandraCluster = new ThriftCluster(CLUSTER_NAME, cassandraHostConfigurator);
		KeyspaceDefinition keyspaceDetail = cassandraCluster.describeKeyspace(KEYSPACE_NAME);
		return (keyspaceDetail != null);
	}
}
