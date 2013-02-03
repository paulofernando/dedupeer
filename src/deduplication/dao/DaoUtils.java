package deduplication.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class DaoUtils {

	/**
	 * Returns a list of Keyspaces
	 * @return List of Keyspaces
	 */
	public static List<String> listKeyspaces(Cassandra.Client client) throws Exception {
		List<String> results = new ArrayList<String>();
		for (KsDef k : client.describe_keyspaces()) {
			results.add(k.getName());
		}
		return results;
	}

	/**
	 * Creates a KsDef CfDef
	 * @param ksname Keyspace name
	 * @param cfname Column Family name
	 * @param replication Replication factor
	 * @return
	 */
	public static KsDef createSimpleKSandCF(String ksname, String cfname, int replication) {
		KsDef newKs = new KsDef();
		newKs.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
		newKs.setName(ksname);		
		newKs.setReplication_factor(replication);
		
		CfDef cfdef = new CfDef();
		cfdef.setKeyspace(ksname);
		cfdef.setName(cfname);
		
		newKs.addToCf_defs(cfdef);
		return newKs;
	}
}
