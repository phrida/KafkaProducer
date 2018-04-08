package com.twitter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;
import twitter4j.Status;

import java.io.IOException;

public class StatusSerializer implements Serializer<Status> {

    private final Logger logger = Logger.getLogger(StatusSerializer.class);

    public void configure(Map map, boolean bln) {
    }

    public byte[] serialize(String string, Status status) {
        ObjectMapper objectMapper = new ObjectMapper();
        //String json_bytes = null;
        byte[] retVal = null;
        try {
            //json_bytes = objectMapper.writeValueAsString(status);
            retVal = objectMapper.writeValueAsString(status).getBytes();
        } catch (JsonProcessingException ex) {
            logger.error("Error in twitter.StatusSerializer.serialize.." + ex.getMessage());
        }
        //return (json_bytes != null) ? json_bytes.getBytes() : null;
        return retVal;
    }

    public void close() {
    }
/*

    public Object deserialize(String string, byte[] bytes) {

        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MyTweetClass tweet_obj = null;
        try {
            tweet_obj = (MyTweetClass) objectMapper.readValue(bytes, MyTweetClass.class);
        } catch (IOException ex) {
            logger.error("Error in MyTweetClassSerDe.deSerialize.." + ex.getMessage());
        }

        return tweet_obj;
    }*/
}
