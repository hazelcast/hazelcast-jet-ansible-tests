package com.hazelcast.jet.tests.sql.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PojoDeserializer implements Deserializer<Pojo> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Pojo deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Pojo pojo = null;
        try {
            pojo = mapper.readValue(data, Pojo.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return pojo;
    }

    @Override
    public void close() {
    }
}
