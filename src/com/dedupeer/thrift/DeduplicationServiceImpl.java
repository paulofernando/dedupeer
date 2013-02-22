package com.dedupeer.thrift;

import java.util.Map;

import org.apache.thrift.TException;

public class DeduplicationServiceImpl implements DeduplicationService.Iface {

	@Override
	public Map<Long, String> deduplicate(Map<Integer, Map<String, String>> chunks, String pathOfFile)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}

}
