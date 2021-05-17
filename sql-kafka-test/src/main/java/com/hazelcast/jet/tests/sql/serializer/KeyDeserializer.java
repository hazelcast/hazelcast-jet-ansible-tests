package com.hazelcast.jet.tests.sql.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.tests.sql.pojo.Key;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KeyDeserializer implements Deserializer<Key> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Key deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Key key = null;
        try {
            key = mapper.readValue(data, Key.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return key;
    }

    @Override
    public void close() {
    }
}
