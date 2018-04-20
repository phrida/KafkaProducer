package com.twitter;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaTwitterProducer {



    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger(KafkaTwitterProducer.class);

        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(10000);

        String consumerKey = "WeJAx0QjHyZuFIuOT0mCAlJqR";
        String consumerSecret = "AF1PYLqk6XPrgFMlYgDQq3l91v6eHpnimlH1u45OSX3yTggMvP";
        String accessToken = "384519993-b2PNRU3TiLxt5gTSUOlUamac7UuHZvWiF2pk9ZqU";
        String accessTokenSecret = "xRnVgh2CpD41GKi0W0B2Z5JA6S8JRIL83W8NuuiuK4CtW";
        String topicName = "twitterdata";


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret)
                .setJSONStoreEnabled(true)
                .setIncludeEntitiesEnabled(true);


        TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
        TwitterStream twitterStream = tf.getInstance();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer",
          //      "com.twitter.StatusSerializer");


        Producer<String, String> producer = new KafkaProducer<>(props);

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                System.out.printf("@%s: %s\n", status.getUser().getScreenName(), status.getText());
                String rawJson = TwitterObjectFactory.getRawJSON(status);
                logger.info(rawJson);

                producer.send(new ProducerRecord<String, String>(topicName, rawJson));



            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {
                System.out.println(stallWarning);

            }

            public void onException(Exception e) {
                e.printStackTrace();

            }
        };
        twitterStream.addListener(listener);

        FilterQuery query = new FilterQuery();
        twitterStream.sample("en");

        //Thread.sleep(5000);

        /*

        while(true) {
            Status tweet = queue.poll();

            if (tweet == null) {
                Thread.sleep(100);
            } else {

                producer.send(new ProducerRecord<String, Status>(topicName, tweet));
            }
        }*/




    }
}
