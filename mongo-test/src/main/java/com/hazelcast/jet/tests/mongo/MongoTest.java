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

package com.hazelcast.jet.tests.mongo;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.mongodb.MongoSinks;
import com.hazelcast.jet.mongodb.MongoSources;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.util.stream.Collectors.toList;

public class MongoTest extends AbstractSoakTest {

    private static final String SINK_LIST_NAME = MongoTest.class.getSimpleName() + "_listSink";
    private static final int DEFAULT_ITEM_COUNT = 10_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 50;
    private static final int SLEEP_BETWEEN_TABLE_READS_SECONDS = 5;

    private static final String MONGO_DATABASE = "SoakTests";
    private static final String COLLECTION_PREFIX = "mongo-test-collection-";
    private static final String DOC_INDEX_PREFIX = "-index-";

    private String mongoConnectionString;
    private int itemCount;
    private List<Integer> inputItems;

    public static void main(String[] args) throws Exception {
        new MongoTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) throws SQLException {
        mongoConnectionString = "mongodb://" + property("mongoIp", "127.0.0.1") + ":27017";
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        inputItems = IntStream.range(0, itemCount).boxed().collect(toList());
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        clearSinkList(client);
        int jobCounter = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            deleteCollection(jobCounter);
            clearSinkList(client);

            executeWriteToMongoPipeline(client, jobCounter);
            executeReadFromMongoPipeline(client, jobCounter);
            assertResults(client, jobCounter);

            clearSinkList(client);
            deleteCollection(jobCounter);

            if (jobCounter % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCounter);
            }

            jobCounter++;
            sleepSeconds(SLEEP_BETWEEN_TABLE_READS_SECONDS);
        }
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }


    private String docId(int collectionCounter, int docIndex) {
        return COLLECTION_PREFIX + collectionCounter + DOC_INDEX_PREFIX + docIndex;
    }

    private void executeWriteToMongoPipeline(HazelcastInstance client, int collectionCounter) {
        final String connectionString = mongoConnectionString;

        Sink<Document> mongoSink = MongoSinks.builder("mongo-stream-sink", Document.class,
                        () -> MongoClients.create(connectionString))
                .into(MONGO_DATABASE, COLLECTION_PREFIX + collectionCounter)
                .identifyDocumentBy("_id", doc -> doc.get("_id"))
                .build();

        Pipeline toMongo = Pipeline.create();
        toMongo.readFrom(TestSources.items(inputItems))
                .map(docIndex -> new Document()
                        .append("docId", "mongo-test-collection-" + collectionCounter + "-index-" + docIndex))
                .rebalance()
                .writeTo(mongoSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("MongoTest_writeTo_" + collectionCounter);
        client.getJet().newJob(toMongo, jobConfig).join();
    }

    private void executeReadFromMongoPipeline(HazelcastInstance client, int collectionCounter) {
        final String connectionString = mongoConnectionString;

        BatchSource<Document> mongoSource = MongoSources
                .batch("mongo-stream-sink", () -> MongoClients.create(connectionString))
                .database(MONGO_DATABASE)
                .collection(COLLECTION_PREFIX + collectionCounter)
                .build();

        Pipeline fromMongo = Pipeline.create();
        fromMongo.readFrom(mongoSource)
                .writeTo(Sinks.list(SINK_LIST_NAME));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("MongoTest_readFrom_" + collectionCounter);
        client.getJet().newJob(fromMongo, jobConfig).join();
    }

    private void assertResults(HazelcastInstance client, int collectionCounter) {
        IList<Document> list = client.getList(SINK_LIST_NAME);
        Set<String> set = new HashSet<>();
        String expected = COLLECTION_PREFIX + collectionCounter + DOC_INDEX_PREFIX;
        for (Document item : list) {
            assertTrue("Does not contain expected part: " + item, item.getString("docId").contains(expected));
            set.add(item.getString("docId"));
        }

        int lastElementNumber = itemCount - 1;
        try {
            assertEquals(itemCount, list.size());
            assertEquals(itemCount, set.size());
            assertTrue(set.contains(docId(collectionCounter, 0)));
            assertTrue(set.contains(docId(collectionCounter, lastElementNumber)));
        } catch (Throwable ex) {
            logger.info("Printing content of incorrect list:");
            for (Document item : list) {
                logger.info(item.toString());
            }
            throw ex;
        }
    }

    private void deleteCollection(int collectionCounter) {
        try (MongoClient mongoClients = MongoClients.create(mongoConnectionString)) {
            mongoClients.getDatabase(MONGO_DATABASE)
                    .getCollection(COLLECTION_PREFIX + collectionCounter).drop();
        }
    }

    private void clearSinkList(HazelcastInstance client) {
        client.getList(SINK_LIST_NAME).clear();
    }

}
