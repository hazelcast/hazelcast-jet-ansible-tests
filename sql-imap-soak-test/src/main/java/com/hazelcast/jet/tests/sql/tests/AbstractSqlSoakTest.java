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

package com.hazelcast.jet.tests.sql.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractSqlSoakTest extends AbstractSoakTest {

    private static final int DATA_SET_SIZE = 65536;

    protected void runTest(HazelcastInstance hazelcastInstance, String mapName) {
        SqlService sql = hazelcastInstance.getSql();
        int index = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            String query = getSqlQuery(index++, mapName);
            System.out.println("Executing query:" + query);     //TODO: Remove this print
            sql.execute(query);
            if(index == DATA_SET_SIZE-1) {
                index = 0;
            }
        }
    }

    protected void populateMap(HazelcastInstance hazelcastInstance, String mapName) {
        IMap<Key, Pojo> map = hazelcastInstance.getMap(mapName);
        Map<Key, Pojo> data = new HashMap<>();

        for (long i = 0; i < DATA_SET_SIZE; i++) {
            data.put(new Key(i), new Pojo(i));
        }
        map.putAll(data);

        assertEquals("Failed to populate the map", DATA_SET_SIZE, map.size());
    }

    protected String getSqlQuery(int key, String mapName) {
        List<String> fields = getFieldNames();
        StringBuilder res = new StringBuilder("SELECT ");

        for (int i = 0; i < fields.size(); i++) {
            if (i != 0) {
                res.append(", ");
            }
            res.append(fields.get(i));
        }
        res.append(" FROM ").append(mapName).append(" WHERE key = ").append(key);

        return res.toString();
    }

    protected static List<String> getFieldNames() {
        return Arrays.asList(
                "key",
                "booleanVal",
                "tinyIntVal",
                "smallIntVal",
                "intVal",
                "bigIntVal",
                "realVal",
                "doubleVal",
                "decimalBigIntegerVal",
                "decimalVal",
                "charVal",
                "varcharVal",
                "dateVal",
                "timeVal",
                "timestampVal",
                "tsTzDateVal",
                "tsTzCalendarVal",
                "tsTzInstantVal",
                "tsTzOffsetDateTimeVal",
                "tsTzZonedDateTimeVal",
                "objectVal",
                "nullVal"
        );
    }
}
