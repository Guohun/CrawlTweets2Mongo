/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package crawltweets2mongo;



import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import static java.lang.Thread.sleep;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.UserMentionEntity;

/**
 *
 * @author uqgzhu1
 */
public class MonoThread implements Runnable {
    
    private DBCollection collection=null;
    private Twitter twitter=null;

    // Display a message, preceded by
    // the name of the current thread

    MonoThread(DBCollection collection, Twitter twitter) {
        this.collection=collection;
        this.twitter=twitter;
    }

    @Override
    public void run() {
        String keyword="university of queensland";
        double latitude=-27.471;
        double longitude=153.0234;
        double radius=200;    
          
        GeoLocation myLoc=new GeoLocation(latitude, longitude);
        GeoLocation GattonLoc=new GeoLocation(-27.560802, 152.344739);
        while (true)
        {
            try {
                System.out.println("catch key word-------------------------");
                getNewTweets(keyword);
                
                sleep(19000);            
                getNewTweets(myLoc,radius);
                sleep(19000);            
                System.out.println("catch Gatton location-------------------------");
                getNewTweets(GattonLoc,100);
                sleep(19000);            
                getNewTweets(" uq ");                
                sleep(19000);            
                getNewTweets("St Lucia");                
                sleep(19000);            
                
                
                
            } catch (InterruptedException ex) {
                java.util.logging.Logger.getLogger(MonoThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }
    void getNewTweets(String keyword)
    {
        try {
            Query query = new Query(keyword);
            query.setCount(20000);
            QueryResult result;
            result = twitter.search(query);
            
            //System.out.println("Getting Tweets..by Key.");
            List<Status> tweets = result.getTweets();
            
            
            
            for (Status tweet : tweets) {
                
                BasicDBObject basicObj = new BasicDBObject();
                basicObj.put("user_Rname", tweet.getUser().getName());
                basicObj.put("user_name", tweet.getUser().getScreenName());
                basicObj.put("retweet_count", tweet.getRetweetCount());
                basicObj.put("tweet_followers_count", tweet.getUser().getFollowersCount());
                
                UserMentionEntity[] mentioned = tweet.getUserMentionEntities();
                basicObj.put("tweet_mentioned_count", mentioned.length);
                basicObj.put("tweet_ID", tweet.getId());
                basicObj.put("tweet_text", tweet.getText());                
                Status temp1=tweet.getRetweetedStatus(); if (temp1!=null)  basicObj.put("Re_tweet_ID", temp1.getUser().getId());
                GeoLocation  loc=tweet.getGeoLocation();
                if (loc!=null) { basicObj.put("Latitude", loc.getLatitude());                basicObj.put("Longitude", loc.getLongitude());                }
                basicObj.put("CreateTime", tweet.getCreatedAt());
                basicObj.put("FavoriteCount", tweet.getFavoriteCount());
                basicObj.put("user_Id", tweet.getUser().getId());
                
                if (tweet.getUser().getTimeZone()!=null) basicObj.put("UsertimeZone", tweet.getUser().getTimeZone());
                if (tweet.getUser().getStatus()!=null)  basicObj.put("UserStatus", tweet.getUser().getStatus());
                //basicObj.put("tweetLocation", tweet.getPlace().getGeometryCoordinates());
                String U_Loc=tweet.getUser().getLocation();                if (U_Loc!=null) basicObj.put("userLocation", U_Loc);
                basicObj.put("number_of_rt", tweet.getRetweetCount());

                
                if (mentioned.length > 0) {
                    basicObj.append("mentions", pickMentions( mentioned));
                }
                try {

                    collection.insert(basicObj);
                } catch (Exception e) {
                    //System.out.println("MongoDB Connection Error : " + e.getMessage());
//                            loadMenu();
                }
            }
            collection.ensureIndex(new BasicDBObject("tweet_ID", 1), new BasicDBObject("unique", true));
        } catch (TwitterException ex) {
            java.util.logging.Logger.getLogger(MonoThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        
}


    void getNewTweets(GeoLocation myLoc,double radius)
    {
        try {
            Query query = new Query();
            Query.Unit unit = Query.KILOMETERS; // or Query.MILES;
            query.setGeoCode(myLoc, radius, unit);            
            if(radius>200)
                query.setCount(20000);
            else
                query.setCount(20000);
            
            QueryResult result;
            result = this.twitter.search(query);
            
            //System.out.println("Getting Tweets. by Geo..");
            List<Status> tweets = result.getTweets();
            
            
            
            for (Status tweet : tweets) {
                
                BasicDBObject basicObj = new BasicDBObject();
                basicObj.put("user_Rname", tweet.getUser().getName());
                basicObj.put("user_name", tweet.getUser().getScreenName());
                basicObj.put("retweet_count", tweet.getRetweetCount());
                basicObj.put("tweet_followers_count", tweet.getUser().getFollowersCount());
                
                UserMentionEntity[] mentioned = tweet.getUserMentionEntities();
                basicObj.put("tweet_mentioned_count", mentioned.length);
                basicObj.put("tweet_ID", tweet.getId());
                basicObj.put("tweet_text", tweet.getText());                
                Status temp1=tweet.getRetweetedStatus(); if (temp1!=null)  basicObj.put("Re_tweet_ID", temp1.getUser().getId());
                GeoLocation  loc=tweet.getGeoLocation();
                if (loc!=null) { basicObj.put("Latitude", loc.getLatitude());                basicObj.put("Longitude", loc.getLongitude());                }
                basicObj.put("CreateTime", tweet.getCreatedAt());
                basicObj.put("FavoriteCount", tweet.getFavoriteCount());
                basicObj.put("user_Id", tweet.getUser().getId());
                
                if (tweet.getUser().getTimeZone()!=null) basicObj.put("UsertimeZone", tweet.getUser().getTimeZone());
                if (tweet.getUser().getStatus()!=null)  basicObj.put("UserStatus", tweet.getUser().getStatus());
                //basicObj.put("tweetLocation", tweet.getPlace().getGeometryCoordinates());
                String U_Loc=tweet.getUser().getLocation();                if (U_Loc!=null) basicObj.put("userLocation", U_Loc);
                basicObj.put("number_of_rt", tweet.getRetweetCount());
                //basicObj.put("isRetweet", tweet.getPlace().getGeometryCoordinates());                
                //basicObj.put("POS", tweet.getWithheldInCountries());
                
                
                if (mentioned.length > 0) {
                    basicObj.append("mentions", pickMentions( mentioned));
                }
                try {
                    //items.insert(basicObj);
                    collection.insert(basicObj);
                } catch (Exception e) {
               //     System.out.println("MongoDB Connection Error : " + e.getMessage());
//                            loadMenu();
                }
            }
            collection.ensureIndex(new BasicDBObject("tweet_ID", 1), new BasicDBObject("unique", true));
        } catch (TwitterException ex) {
            java.util.logging.Logger.getLogger(MonoThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    private  String[] pickMentions(UserMentionEntity[] mentioned){
      
      LinkedList<String> friends = new LinkedList<String>();
      
      for (UserMentionEntity  x: mentioned ) {                    
              //String  y=x.getName();// Long.toString(x.getId());
              friends.add(x.getScreenName());               
      }
      String a[] = {};
      return  friends.toArray(a);
    }

}