package acn.nosql.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class TheMain {
	final static Logger LOG = LoggerFactory.getLogger(TheMain.class);
	public static final String KEYSPACE = "trainingks";
	
	public static void main(String[] args) {
		LOG.info("NoSQL Cassandra TestApp kicking off  = Hello World!");
		
		// Step 1: Buildup a Connection to localhost via DataStax API
		// our helper function also registers a nice shutdown hook, so
		// we don't have to worry about the connection
		final Cluster cluster = MachineRoom.connectToCassandra("127.0.0.1");
		
		// Step 2: Let's prepare our keyspace
		MachineRoom.prepareKeyspace(cluster);
		
		/* Step 3: Buildup Session for App-Use
		 * please note that the "session" here maintains multiple
		 * connections, is thread-safe, so one is enough per application
		 * and keyspace.
		 */
		final Session cassandra = cluster.connect(KEYSPACE);
		
		// Add static sites, hello world and shutdown routs:
		MachineRoom.addRoutes(cassandra);
		// and here we go:
		TweetsAPI.addRoutes(cassandra);
	}
}
