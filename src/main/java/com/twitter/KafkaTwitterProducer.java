package com.twitter;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.net.Socket;
import java.nio.Buffer;
import java.util.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaTwitterProducer {



    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger(KafkaTwitterProducer.class);

        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(10000);

        /*String consumerKey = "WeJAx0QjHyZuFIuOT0mCAlJqR";
        String consumerSecret = "AF1PYLqk6XPrgFMlYgDQq3l91v6eHpnimlH1u45OSX3yTggMvP";
        String accessToken = "384519993-b2PNRU3TiLxt5gTSUOlUamac7UuHZvWiF2pk9ZqU";
        String accessTokenSecret = "xRnVgh2CpD41GKi0W0B2Z5JA6S8JRIL83W8NuuiuK4CtW";*/
        String topicName = "twitterparse";


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("WeJAx0QjHyZuFIuOT0mCAlJqR")
                .setOAuthConsumerSecret("AF1PYLqk6XPrgFMlYgDQq3l91v6eHpnimlH1u45OSX3yTggMvP")
                .setOAuthAccessToken("384519993-b2PNRU3TiLxt5gTSUOlUamac7UuHZvWiF2pk9ZqU")
                .setOAuthAccessTokenSecret("xRnVgh2CpD41GKi0W0B2Z5JA6S8JRIL83W8NuuiuK4CtW")
                .setJSONStoreEnabled(true);




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



        Producer<String, String> producer = new KafkaProducer<>(props);

        Socket clientSocket = null;
        BufferedReader input = null;

        try {
            clientSocket = new Socket("localhost", 10001);
            input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            System.out.println(clientSocket.getPort());

            if (clientSocket != null && input != null) {
                try {
                    String responseLine;
                    while ((responseLine = input.readLine()) != null) {
                        System.out.println("Server: " + responseLine);
                        //Status status = TwitterObjectFactory.createStatus(responseLine);

                        //System.out.printf("@%s: %s\n", status.getUser().getScreenName(), status.getText());
                        //String rawJson = TwitterObjectFactory.getRawJSON(responseLine);
                        //System.out.println(rawJson);
                        //logger.info(rawJson);

                        producer.send(new ProducerRecord<String, String>(topicName, responseLine));

                    }

                } catch (IOException e) {
                    System.out.println(e);
                }
            }

        } catch (IOException e) {
            System.out.println(e);
        }

        try {
            input.close();
            clientSocket.close();
        } catch (IOException e) {
            System.out.println(e);
        }





        /*
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                System.out.printf("@%s: %s\n", status.getUser().getScreenName(), status.getText());
                String rawJson = TwitterObjectFactory.getRawJSON(status);
                System.out.println(rawJson);
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

        twitterStream.sample("en");

        //Thread.sleep(5000);*/

/*

        while(true) {
            Status status = queue.poll();

            if (status == null) {
                Thread.sleep(100);
            } else {
                System.out.printf("@%s: %s\n", status.getUser().getScreenName(), status.getText());
                String rawJson = TwitterObjectFactory.getRawJSON(status);
                System.out.println(rawJson);

                producer.send(new ProducerRecord<String, String>(topicName, rawJson));
            }
        }*/




    }
}
