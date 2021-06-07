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

package com.hazelcast.jet.tests.sql.common;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class NonEqualJoinExecutor extends BaseExecutor implements Callable<Integer> {

    private static final String MAP_NAME = "my_non_equal_map";
    private static final int MAP_SIZE = 10000;

    private IMap<Key, Pojo> myMap;
    private ILogger logger;
    private long begin;
    private long durationInMillis;

    private HazelcastInstance client;

    public NonEqualJoinExecutor(HazelcastInstance client, ILogger logger, long begin,
                                long durationInMillis, long threshold) {
        super(logger, begin, threshold);
        this.client = client;
        this.logger = logger;
        this.begin = begin;
        this.durationInMillis = durationInMillis;

        myMap = client.getMap(MAP_NAME);
        createSqlMapping();
        populateMap(myMap, MAP_SIZE);
    }

    public Integer call() {
        logger.info("Execute query: " + getSqlQuery());
        Iterator<SqlRow> iterator = client.getSql().execute(getSqlQuery()).iterator();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            SqlRow sqlRow = iterator.next();
            long longValue = sqlRow.getObject("v");
            verifyNotStuck();
            currentQueryCount++;
            printProgress("non equal join");
        }
        return currentQueryCount;
    }

    public String getSqlQuery() {
        return String.format("SELECT * FROM TABLE(generate_stream(1)) streaming LEFT JOIN %s " +
                "AS map ON CAST(map.bigIntVal AS BIGINT) BETWEEN %d AND %d",
                MAP_NAME, getBetweenFromValue(), getBetweenToValue());
    }

    private int getBetweenFromValue() {
        return ThreadLocalRandom.current().nextInt(0, MAP_SIZE / 2);
    }

    private int getBetweenToValue() {
        return ThreadLocalRandom.current().nextInt((MAP_SIZE / 2) + 1, MAP_SIZE - 1);
    }

    private void createSqlMapping() {
        client.getSql().execute("CREATE MAPPING " + MAP_NAME + "(" +
                " booleanVal BOOLEAN," +
                " tinyIntVal TINYINT," +
                " smallIntVal SMALLINT," +
                " intVal INT," +
                " bigIntVal BIGINT," +
                " realVal REAL," +
                " doubleVal DOUBLE," +
                " decimalVal DECIMAL," +
                " varcharVal VARCHAR)" +
                " TYPE IMap" +
                " OPTIONS (" +
                " 'keyFormat' = 'java'," +
                " 'keyJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Key'," +
                " 'valueFormat' = 'java'," +
                " 'valueJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Pojo'" +
                ")");
    }
}
