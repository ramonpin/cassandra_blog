package acn.nosql.cassandra;

import static spark.Spark.*;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Request;
import spark.Response;
import spark.Route;
import acn.nosql.cassandra.pojo.Tweet;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class TweetsAPI {
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private static final Logger log = LoggerFactory.getLogger(TweetsAPI.class);
	
	public static void addRoutes(final Session cassandra) {
        get(new Route("/tweets/:username") {
            @Override
            public Object handle(Request request, Response response) {
            	String username = (String) request.params("username");
            	
            	if(username == null || username.isEmpty()) {
            		halt(500);
            	} else {
            		username = username.trim().toLowerCase();
            	}
            	
            	List<Tweet> tweets = retrieveTweetsOfUser(cassandra, username);
            	
            	return MachineRoom.returnAsJSON(response, tweets);
            }
        });
        post(new Route("/tweets", "application/json") {
			@Override
			public Object handle(Request request, Response response) {
				try {
					Tweet tweet = gson.fromJson(request.body(), Tweet.class);
					upsertTweet(cassandra, tweet);
					log.info("Saved {}", request.body());
					return String.format("Saved @ %s", new Date());
				} catch(JsonSyntaxException e) {
					log.error("Error parsing JSON", e);
					halt(500);
				}
				return null;
			}
        });
	}
	
	public static boolean upsertTweet(final Session cassandra, Tweet tweet) {
		Statement insert = insertInto("tweets")
				.value("username", tweet.username)
				.value("time", tweet.time == null ? new Date() : tweet.time)
				.value("content", tweet.content);
		
		cassandra.execute(insert);
		return true;
	}
	
	public static List<Tweet> retrieveTweetsOfUser(final Session cassandra, String username) {
		List<Tweet> tweets = new LinkedList<Tweet>();
		
		Statement query = select().all().from("tweets")
				.where(eq("username", username))
				.orderBy(desc("time"))
				.limit(10);
		
		log.info("Getting tweets by {}: {}", username, query);
		
		ResultSet res = cassandra.execute(query);
		for(Row row: res) {
			Tweet tweet = new Tweet();
			tweet.username = username;
			tweet.time = row.getDate("time");
			tweet.content = row.getString("content");
			tweets.add(tweet);
		}
		
		return tweets;
	}
}
