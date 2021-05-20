/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
