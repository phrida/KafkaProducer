package com.twitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaTwitterProducer {



    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger(KafkaTwitterProducer.class);

        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(10000);

        String topicName = "twittergenerator";


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("consumerKey")
                .setOAuthConsumerSecret("consumerSecret")
                .setOAuthAccessToken("accessToken")
                .setOAuthAccessTokenSecret("accessTokenSecret")
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

        //When reading from Data Generator

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




        //When reading from Twitter Streaming API

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


    }
}
