/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package crawltweets2mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import static java.lang.Thread.sleep;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author uqgzhu1
 */
public class CrawlTweets2Mongo {

    static Twitter twitter = null;
    static DBCollection collection = null;

    static void threadMessage(String message) {
        String threadName = Thread.currentThread().getName();
        System.out.format("%s: %s%n", threadName, message);
    }

    public static void getTweetByQuery(boolean loadRecords, String keyword) {

        if (collection != null) {
            final String api_key = "your api key in here";
            final String api_secret = "applied it from twitter website";
            final String access_token = "applied token from twitter website";
            final String access_token_secret = "applied it from twitter website";
            // The factory instance is re-useable and thread safe.
            if (twitter == null) {
                ConfigurationBuilder cb = new ConfigurationBuilder();
                cb.setDebugEnabled(false)
                        .setOAuthConsumerKey(api_key)
                        .setOAuthConsumerSecret(api_secret)
                        .setOAuthAccessToken(access_token)
                        .setOAuthAccessTokenSecret(access_token_secret);
                TwitterFactory tf = new TwitterFactory(cb.build());
                twitter = tf.getInstance();
            }

            threadMessage("Starting MessageLoop thread");
            long startTime = System.currentTimeMillis();
            Thread t = new Thread(new MonoThread(collection, twitter));
            t.start();
            threadMessage("Waiting for MessageLoop thread to finish");
            // loop until MessageLoop           // thread exits
            while (t.isAlive()) {
                try {
                    t.join(28000);

                    t.join(28000);

                    t.join(28000);
                    if (((System.currentTimeMillis() - startTime) > 1000 * 60 * 60) // long patience = 1000 * 60 * 60;
                            && t.isAlive()) {
                        threadMessage("Tired of waiting!");
                        t.interrupt();
                        t.join();
                    }
                } catch (InterruptedException ex) {
                    Logger.getLogger(CrawlTweets2Mongo.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            threadMessage("Finally!");
        } else {
            System.out.println("MongoDB is not Connected! Please check mongoDB intance running..");
        }

    }

    public static void main(String[] args) {

        try {

            // Connect to mongodb
            MongoClient mongo = new MongoClient("localhost", 27017);

            // get database
            // if database doesn't exists, mongodb will create it for you
            DB db = mongo.getDB("test");

            // get collection
            // if collection doesn't exists, mongodb will create it for you
            collection = db.getCollection("twitter");
            getTweetByQuery(true, "bus");

            collection = null;
            db = null;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (MongoException e) {
            e.printStackTrace();
        }

    }
    //----------------------------------------------------
  

}
