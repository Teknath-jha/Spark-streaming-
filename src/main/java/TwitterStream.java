
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.google.common.collect.Iterables;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.File;
import java.io.PipedWriter;
import java.io.PrintWriter;

public class TwitterStream {

	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub.

		final SparkConf sparkConf = new SparkConf().setAppName("twitter-stream-tkjha").setMaster("local[*]");

		final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(100));

		final Configuration conf = new ConfigurationBuilder().setDebugEnabled(false)
				.setOAuthConsumerKey("put you keys from your own developer account ")
				.setOAuthConsumerSecret("put you keys from your own developer account ")
				.setOAuthAccessToken("put you keys from your own developer account ")
				.setOAuthAccessTokenSecret("put you keys from your own developer account ")
				.build();

		final Authorization twitterAuth = new OAuthAuthorization(conf);

		final JavaReceiverInputDStream<Status> inputDStream = TwitterUtils.createStream(streamingContext, twitterAuth,
				new String[] {});

		final JavaPairDStream<String, String> userTweetsStream = inputDStream.mapToPair(

				(status) -> new Tuple2<String, String>(status.getUser().getScreenName(), status.getText()));

		final JavaPairDStream<String, Iterable<String>> tweetsReducedByUser = userTweetsStream.groupByKey();

		final JavaPairDStream<String, Integer> tweetsMappedByUser = tweetsReducedByUser.mapToPair(
				userTweets -> new Tuple2<String, Integer>(userTweets._1, Iterables.size(userTweets._2)));

		System.out.println("66_1");

		tweetsMappedByUser.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
			pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {

				/**
				 * 
				 */

				private static final long serialVersionUID = 1L;

				public void call(Tuple2<String, Integer> t) {
					System.out.println(t._1() + " , " + t._2());

				}

			});

		});

		streamingContext.start();
		System.out.println("****start**************************************************");
		streamingContext.awaitTermination();

		System.out.println("****End**************************************************");
	}

}
