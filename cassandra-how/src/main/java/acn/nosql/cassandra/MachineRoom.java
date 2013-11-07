package acn.nosql.cassandra;

import static spark.Spark.*;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Filter;
import spark.Request;
import spark.Response;
import spark.Route;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MachineRoom {
	private final static Logger LOG = LoggerFactory.getLogger(MachineRoom.class);
	private final static Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public static Cluster connectToCassandra(String node) {
		Cluster cluster = Cluster.builder()
			         .addContactPoint(node)
			         .build();
	
		Metadata metadata = cluster.getMetadata();
		TheMain.LOG.info("Connected to cluster: {}",
				metadata.getClusterName());

		// Register a Shutdown-Hook:
		closeClusterOnShutdown(cluster);
		
		return cluster;
	}
	
	private static void closeClusterOnShutdown(final Cluster cluster) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {
				// Close Cassandra connection, this will also
		    	// close down ALL open sessions!
				cluster.shutdown();
		    }
		});
	}

	public static class DelayedShutdown extends Thread {
		private long waittime;
		
		private DelayedShutdown(long waittime) {
			this.waittime = waittime;
		}
		
		public static void asyncInMsec(long waittime) {
			new DelayedShutdown(waittime).start();
		}
		
		@Override
		public void run() {
			LOG.info("Ladies and Gentleman, we're shutting down...");
			// wait a configured time so web-browser get's message back
			try {
				Thread.sleep(waittime);
			} catch (InterruptedException e) {
				LOG.warn("Lazy shutdown interrupted", e);
			}
			Runtime.getRuntime().exit(0);
		}
	}
	
	public static void addRoutes(final Session cassandra) {
        staticFileLocation("/static");
        
        // Filter to enable CORS
        after(new Filter() {
           @Override
           public void handle(Request request, Response response) {
           	response.header("foo", "set by after filter");
           }
        });

        get(new Route("/hello") {
            @Override
            public Object handle(Request request, Response response) {
                response.type("text/plain");
            	StringBuilder builder = new StringBuilder();
        		builder.append("Stats as of ").append(new Date()).append('\n');
        		builder.append("Running on Java ").append(System.getProperty("java.version")).append('\n');
        		builder.append('\n');
                
                // Cluster meta-data:
            	Cluster cluster = cassandra.getCluster();
        		Metadata metadata = cluster.getMetadata();
        		builder.append(String.format("Connected to Cassandra cluster: %s\n",
        				metadata.getClusterName()));
        		for (Host host : metadata.getAllHosts()) {
        			builder.append(String.format("Datacenter: %s Host: %s Rack: %s\n",
        					host.getDatacenter(), host.getAddress(), host.getRack()));
        		}
        		builder.append('\n');
        		
        		// Some info about Keyspaces:
        		builder.append("Keyspaces:\n");
        		Statement query = QueryBuilder.select().all().from("system", "schema_keyspaces");
        		LOG.debug("Retrieving keyspaces: {}", query.toString());
        		ResultSet res = cassandra.execute(query);
        		for(Row row: res) {
        			builder.append(row.getString("keyspace_name")).append(": ");
        			builder.append(row.getBool("durable_writes") ? "DURABLE" : "NOT DURABLE").append(", ");
        			builder.append(row.getString("strategy_class")).append(", ");
        			builder.append(row.getString("strategy_options")).append("\n");
        		}
        		
                return builder.toString();
            }
        });

        get(new Route("/shutdown") {
            @Override
            public Object handle(Request request, Response response) {
            	MachineRoom.DelayedShutdown.asyncInMsec(200);
                response.type("text/plain");
            	return String.format("Good Bye!\nServer Shutdown @ %s", new Date());
            }
        });
	}

	public static void prepareKeyspace(Cluster cluster) {
		Session session = cluster.connect();
		
		// using the new CQL3 conditional keyspace creation!
		session.execute(
				"CREATE KEYSPACE IF NOT EXISTS " + TheMain.KEYSPACE + " WITH " + 
				"replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		
		// switch into keyspace
		session.execute("USE " + TheMain.KEYSPACE);
		
		session.execute(
			"CREATE TABLE IF NOT EXISTS tweets (         " +
		    "  username text,                            " +
			"  time timestamp,                           " +
		    "  content text,                             " +
			"  PRIMARY KEY (username, time)              " + 
		    ") WITH CLUSTERING ORDER BY (time DESC)      ");
		
		session.execute(
			"CREATE TABLE IF NOT EXISTS follows (        " +
		    "  username text,                            " +
			"  following set<text>,                      " +
			"  PRIMARY KEY (username)                    " + 
		    ")                                           ");
		
		session.execute(
			"CREATE TABLE IF NOT EXISTS followers (      " +
		    "  username text,                            " +
			"  who set<text>,                      " +
			"  PRIMARY KEY (username)                    " + 
		    ")                                           ");

		session.shutdown();
	}
	
	public static final String returnAsJSON(Response response, Object obj) {
        response.type("application/json");
		return gson.toJson(obj);
	}
}
