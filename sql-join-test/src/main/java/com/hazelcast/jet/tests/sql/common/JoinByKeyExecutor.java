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
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.sql.SqlRow;

import java.util.Iterator;
import java.util.concurrent.Callable;

import static java.util.concurrent.locks.LockSupport.parkNanos;

public class JoinByKeyExecutor extends BaseExecutor implements Callable<Integer> {

    private static final String MAP_NAME = "my_key_map";
    private IMap<Key, Pojo> myMap;
    private ILogger logger;
    private long begin;
    private long durationInMillis;
    private long threshold;

    private HazelcastInstance client;

    public JoinByKeyExecutor(HazelcastInstance client, ILogger logger, long begin,
                             long durationInMillis, long threshold) {
        super(logger, begin, threshold);
        this.client = client;
        this.logger = logger;
        this.begin = begin;
        this.durationInMillis = durationInMillis;
        this.threshold = threshold;

        myMap = client.getMap(MAP_NAME);
        createSqlMapping();
        populateMap(myMap, 1000);
    }

    public Integer call() {
        logger.info("Execute query: " + getSqlQuery());
        Iterator<SqlRow> iterator = client.getSql().execute(getSqlQuery()).iterator();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            SqlRow sqlRow = iterator.next();
            long longValue = sqlRow.getObject("v");
            currentQueryCount++;
            verifyNotStuck();
            lastCheckpoint = System.currentTimeMillis();
            printProgress("join by key");
            parkNanos(threshold);
        }
        return currentQueryCount;
    }

    public String getSqlQuery() {
        return "SELECT * FROM TABLE(generate_stream(10)) streaming LEFT JOIN " + MAP_NAME + " " +
                "AS map ON map.__key=streaming.v";
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
                " 'keyFormat' = 'bigint'," +
                " 'keyJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Key'," +
                " 'valueFormat' = 'java'," +
                " 'valueJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Pojo'" +
                ")");
    }
}
