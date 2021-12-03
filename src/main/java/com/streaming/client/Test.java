/**
 * 
 */
package com.streaming.client;



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


public class Test {


	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub.

		
		final SparkConf sparkConf =  new SparkConf().setAppName("twitter-stream-tkjha").setMaster("local[*]");
		
		
		final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(10));
		
		final Configuration conf =  new ConfigurationBuilder().setDebugEnabled(false)
																					.setOAuthConsumerKey("7ir3F9clD3UUHHW5MqUhpx2ek")
																					.setOAuthConsumerSecret("0tiWz2qClfJkJkLrt6DUauwqnKFqvbFX7Kt3VNWzRINQQoU5gP")
																					.setOAuthAccessToken("AAAAAAAAAAAAAAAAAAAAACIJVgEAAAAAXfFlRi%2Fz0248FWkpavpG8dgFcCo%3DatiPLX87VDH1SlWB1bOvJVSdAz02vQNdI0CwhaoJoyjPSiLPYb")
																					.setOAuthAccessTokenSecret("1456851319059148801-sIe6MR0IW8AmVJUqnrKMZVVjBhDuLe")
																					.build();
		
																					
																					
		final Authorization twitterAuth = new OAuthAuthorization(conf);
		
		
		final JavaReceiverInputDStream<Status> inputDStream = TwitterUtils.createStream(streamingContext, twitterAuth , new String[]{});
		
		final JavaPairDStream<String,String> userTweetsStream = inputDStream.mapToPair(
																						(status) -> new Tuple2<String ,String>(status.getUser().getScreenName(), status.getText()));
		
		
		final JavaPairDStream<String , Iterable<String>> tweetsReducedByUser = userTweetsStream.groupByKey();
		
		for(int i=0;i<10;i++)
			System.out.println(i);
		System.out.println(tweetsReducedByUser + " " + userTweetsStream);
		
		
			
		
		final JavaPairDStream<String , Integer> tweetsMappedByUser = tweetsReducedByUser.mapToPair(
				userTweets -> new Tuple2<String,Integer>(userTweets._1, Iterables.size(userTweets._2))
				);


		tweetsMappedByUser.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>)pairRDD -> {
			pairRDD.foreach(new VoidFunction<Tuple2<String,Integer>>(){
				
				/**
				 * 
				 */
				private  static final long serialVersionUID = 1L;
								
				@Override
				public void call(Tuple2<String, Integer> t)throws Exception{
					System.out.println(t._1() + " , " + t._2());
				}
			});
			
			
			
		});		
		
streamingContext.start();

streamingContext.awaitTermination();
	}


}
		
		
		
		