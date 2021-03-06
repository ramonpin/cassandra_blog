package acn.nosql.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.datastax.driver.core.querybuilder.QueryBuilder.add;
import static spark.Spark.get;
import static spark.Spark.post;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Request;
import spark.Response;
import spark.Route;
import acn.nosql.cassandra.pojo.Follow;
import acn.nosql.cassandra.pojo.Tweet;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class TweetsAPI {
	private static final Gson gson = new GsonBuilder().setPrettyPrinting()
			.create();
	private static final Logger log = LoggerFactory.getLogger(TweetsAPI.class);

	public static void addRoutes(final Session cassandra) {
		get(new Route("/tweets/:username") {
			@Override
			public Object handle(Request request, Response response) {
				String username = (String) request.params("username");

				if (username == null || username.isEmpty()) {
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
				} catch (JsonSyntaxException e) {
					log.error("Error parsing JSON", e);
					halt(500);
				}
				return null;
			}
		});
		post(new Route("/follow", "application/json") {
			@Override
			public Object handle(Request request, Response response) {
				Follow follow = gson.fromJson(request.body(), Follow.class);
				follow(cassandra, follow);
				log.info("Following {}", request.body());
				return String.format("Now '%s' follows '%s'.", 
						follow.username,
						follow.followed);
			}

		});
		post(new Route("/unfollow", "application/json") {
			@Override
			public Object handle(Request request, Response response) {
				Follow follow = gson.fromJson(request.body(), Follow.class);
				unfollow(cassandra, follow);
				log.info("Unfollowing {}", request.body());
				return String.format("Now '%s' unfollows '%s'.", 
						follow.username,
						follow.followed);
			}

		});
	}

	protected static boolean follow(Session cassandra, Follow follow) {
		
		Set<String> set = new HashSet<String>();
		
		PreparedStatement psFollows = cassandra.prepare(
				"UPDATE follows " +
	               "SET following = following + ? " + 
		         "WHERE username = ?");
		
		set.add(follow.followed);
		cassandra.execute(psFollows.bind(set, follow.username));
		
		PreparedStatement psFollowers = cassandra.prepare(
				"UPDATE followers " +
	               "SET who = who + ? " + 
		         "WHERE username = ?");
		
		// Usando el query-builder sería así
		// *************************************************************
		// cassandra.execute(update("followers")
		//                  .where(eq("username", follow.username))
		//                  .with(add("who", follow.followed)));
		
		set.clear();
		set.add(follow.username);
		cassandra.execute(psFollowers.bind(set, follow.followed));
		
		return true;
	}

	protected static boolean unfollow(Session cassandra, Follow follow) {
		
		Set<String> set = new HashSet<String>();
		
		PreparedStatement psFollows = cassandra.prepare(
				"UPDATE follows " +
	               "SET following = following - ? " + 
		         "WHERE username = ?");
		
		set.add(follow.followed);
		cassandra.execute(psFollows.bind(set, follow.username));
		
		PreparedStatement psFollowers = cassandra.prepare(
				"UPDATE followers " +
	               "SET who = who - ? " + 
		         "WHERE username = ?");
		
		
		
		set.clear();
		set.add(follow.username);
		cassandra.execute(psFollowers.bind(set, follow.followed));
		
		return true;
	}

	public static boolean upsertTweet(final Session cassandra, Tweet tweet) {
		cassandra.execute(
			insertInto("tweets")
				.value("username", tweet.username)
				.value("time", tweet.time == null ? new Date() : tweet.time)
				.value("content", "@" + tweet.username + ": " + tweet.content));
		
		// Update other timelines
		ResultSet followers = cassandra.execute( 
			select("who")
				.from("followers")
				.where(eq("username", tweet.username)));

		for(Row follower: followers.all()) {
			for(String username: follower.getSet(0, String.class)) {
				cassandra.executeAsync(
					insertInto("tweets")
				    	.value("username", username)
				    	.value("time", tweet.time == null ? new Date() : tweet.time)
				    	.value("content", "@" + tweet.username + ": " + tweet.content));
		    }
		}
		return true;
	}

	public static List<Tweet> retrieveTweetsOfUser(final Session cassandra,
			String username) {
		List<Tweet> tweets = new LinkedList<Tweet>();

		Statement query = select().all().from("tweets")
				.where(eq("username", username)).orderBy(desc("time"))
				.limit(10);

		log.info("Getting tweets by {}: {}", username, query);

		ResultSet res = cassandra.execute(query);
		for (Row row : res) {
			Tweet tweet = new Tweet();
			tweet.username = username;
			tweet.time = row.getDate("time");
			tweet.content = row.getString("content");
			tweets.add(tweet);
		}

		return tweets;
	}
}
