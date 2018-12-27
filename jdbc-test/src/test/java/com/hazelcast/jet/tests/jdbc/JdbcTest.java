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

package com.hazelcast.jet.tests.jdbc;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.IQueue;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.server.JetBootstrap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnit4.class)
public class JdbcTest {

    private static final String DB_AND_USER = "/soak-test?user=root&password=soak-test";
    private static final String QUEUE_NAME = JdbcTest.class.getSimpleName();
    private static final int PERSON_COUNT = 20_000;


    private JetInstance jet;
    private String connectionUrl;
    private long durationInMillis;

    public static void main(String[] args) {
        JUnitCore.main(JdbcTest.class.getName());
    }

    @Before
    public void setup() throws SQLException {
        System.setProperty("hazelcast.logging.type", "log4j");
        connectionUrl = System.getProperty("connectionUrl", "jdbc:mysql://localhost") + DB_AND_USER;
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "30")));
        jet = JetBootstrap.getInstance();
        Config config = jet.getHazelcastInstance().getConfig();
        config.addQueueConfig(new QueueConfig().setName(QUEUE_NAME).setMaxSize(PERSON_COUNT * 2));

        createAndFillTable();
    }

    @After
    public void cleanup() {
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void test() throws Exception {
        Sink<String> sink = SinkBuilder
                .sinkBuilder("queueSink", c -> c.jetInstance().getHazelcastInstance().getQueue(QUEUE_NAME))
                .<String>receiveFn(IQueue::put)
                .preferredLocalParallelism(1)
                .build();

        StreamSource<Entry<Long, String>> source = SourceBuilder
                .stream("queueSource", QueueSource::new)
                .fillBufferFn(QueueSource::addToBuffer)
                .distributed(1)
                .build();


        Pipeline p1 = Pipeline.create();
        p1.drawFrom(source)
          .drainTo(Sinks.jdbc("INSERT INTO PERSON_ALL(id, name) VALUES(?, ?)", connectionUrl,
                  (stmt, entry) -> {
                      stmt.setLong(1, entry.getKey());
                      stmt.setString(2, entry.getValue());
                  }
          ));

        Pipeline p2 = Pipeline.create();
        p2.drawFrom(Sources.jdbc(connectionUrl, "select * from PERSON",
                resultSet -> resultSet.getString(2)))
          .drainTo(sink);

        Job streamJob = jet.newJob(p1, new JobConfig().setName("JDBC Test stream queue to table"));

        int jobCounter = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            jet.newJob(p2, new JobConfig().setName("JDBC Test read table to queue [" + jobCounter + "]")).join();
            assertNotEquals(FAILED, streamJob.getStatus());
            jobCounter++;
        }

        assertTableCount(jobCounter * PERSON_COUNT);

        streamJob.cancel();

    }

    private void assertTableCount(long expectedTableCount) throws SQLException, InterruptedException {
        IQueue<String> queue = jet.getHazelcastInstance().getQueue(QUEUE_NAME);
        for (int i = 0; i < 100; i++) {
            if (queue.size() == 0 && expectedTableCount == getTableCount()) {
                return;
            }
            SECONDS.sleep(1);
        }
        throw new AssertionError("Table count (" + getTableCount() + " should be " + expectedTableCount);
    }

    private void createAndFillTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE PERSON(id int primary key, name varchar(255))");
            statement.execute("CREATE TABLE PERSON_ALL(id int primary key, name varchar(255))");
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO PERSON(id, name) VALUES(?, ?)")) {
                for (int i = 1; i <= PERSON_COUNT; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, "name-" + i);
                    stmt.executeUpdate();
                }
            }
        }
    }

    private long getTableCount() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT count(*) from PERSON_ALL");
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    static class QueueSource {

        private static final int MAX_ELEMENTS = 100;

        private final List<String> tempList = new ArrayList<>(MAX_ELEMENTS);

        private final IQueue<String> queue;
        private final int totalParallelism;

        private long counter;

        QueueSource(Context context) {
            queue = context.jetInstance().getHazelcastInstance().getQueue(QUEUE_NAME);
            totalParallelism = context.totalParallelism();
            counter = context.globalProcessorIndex();
        }

        void addToBuffer(SourceBuffer<Entry<Long, String>> buffer) {
            queue.drainTo(tempList, MAX_ELEMENTS);
            for (String item : tempList) {
                buffer.add(Util.entry(counter, item));
                counter += totalParallelism;
            }
            tempList.clear();
        }

    }

}
