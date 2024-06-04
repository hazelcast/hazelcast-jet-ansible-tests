/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.mongodb.MongoSinks;
import com.hazelcast.jet.mongodb.MongoSources;
import com.hazelcast.jet.mongodb.impl.MongoUtilities;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.mongodb.ResourceChecks.NEVER;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

public class MongoTest extends AbstractJetSoakTest {
    private static final int DEFAULT_ITEM_COUNT = 5_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 50;
    private static final int SLEEP_BETWEEN_READS_SECONDS = 2;
    private static final int JOB_STATUS_ASSERTION_ATTEMPTS = 1200;
    private static final int JOB_STATUS_ASSERTION_SLEEP_MS = 100;
    private static final int STREAM_SINK_ASSERTION_ATTEMPTS = 120;
    private static final int STREAM_SINK_ASSERTION_SLEEP_MS = 1000;

    private static final String MONGO_DATABASE = MongoTest.class.getSimpleName();
    private static final String COLLECTION_PREFIX = "collection_";
    private static final String DOC_PREFIX = "mongo-document-from-collection-";
    private static final String DOC_COUNTER_PREFIX = "-counter-";
    private static final String STREAM_READ_FROM_PREFIX = MongoTest.class.getSimpleName() + "_streamReadFrom_";
    private static final String WRITE_PREFIX = MongoTest.class.getSimpleName() + "_write_";
    private static final String BATCH_READ_FROM_PREFIX = MongoTest.class.getSimpleName() + "_batchReadFrom_";
    private static final String BATCH_SINK_LIST_NAME = MongoTest.class.getSimpleName() + "_listSinkBatch";
    private static final String STREAM_SINK_LIST_NAME = MongoTest.class.getSimpleName() + "_listSinkStream";


    private String mongoConnectionString;
    private int itemCount;
    private List<Integer> inputItems;
    private MongoClient mongoClient;

    public static void main(final String[] args) throws Exception {
        new MongoTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) {
        mongoConnectionString = "mongodb://" + property("mongoIp", "127.0.0.1") + ":27017";
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        inputItems = IntStream.range(0, itemCount).boxed().collect(toList());
        final MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoConnectionString))
                .applyToConnectionPoolSettings(builder -> builder
                        .minSize(10)
                        .maxConnectionIdleTime(1, MINUTES)).build();
        mongoClient = MongoClients.create(mongoClientSettings);
    }

    @Override
    public void test(final HazelcastInstance client, final String name) {
        clearSinks(client);
        int jobCounter = 0;
        final long begin = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                deleteCollectionAndCreateNewOne(jobCounter);
                clearSinks(client);

                startStreamReadFromMongoPipeline(client, jobCounter);
                executeWriteToMongoPipeline(client, jobCounter);
                executeBatchReadFromMongoPipeline(client, jobCounter);

                assertBatchResults(client, jobCounter);
                assertStreamResults(client, jobCounter);

                stopStreamRead(client, jobCounter);
                clearSinks(client);
                deleteCollectionAndCreateNewOne(jobCounter);

                if (jobCounter % LOG_JOB_COUNT_THRESHOLD == 0) {
                    logger.info("Job count: " + jobCounter);
                }

                jobCounter++;
                sleepSeconds(SLEEP_BETWEEN_READS_SECONDS);
            }
        } finally {
            logger.info("Test finished with job count: " + jobCounter);
            mongoClient.close();
        }
    }

    @Override
    protected void teardown(final Throwable t) {
    }

    private static String docId(final int collectionCounter, final int docCounter) {
        return DOC_PREFIX + collectionCounter + DOC_COUNTER_PREFIX + docCounter;
    }

    private static void stopStreamRead(final HazelcastInstance client, final int collectionCounter) {
        client.getJet().getJob(STREAM_READ_FROM_PREFIX + collectionCounter).cancel();
    }

    private static void clearSinks(final HazelcastInstance client) {
        client.getList(BATCH_SINK_LIST_NAME).clear();
        client.getList(STREAM_SINK_LIST_NAME).clear();
    }

    private static void assertJobStatusEventually(final Job job) {
        for (int i = 0; i < JOB_STATUS_ASSERTION_ATTEMPTS; i++) {
            if (job.getStatus().equals(RUNNING)) {
                return;
            } else {
                sleepMillis(JOB_STATUS_ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError("Job " + job.getName() + " does not have expected status: " + RUNNING
                + ". Job status: " + job.getStatus());
    }

    private void deleteCollectionAndCreateNewOne(final int collectionCounter) {
        mongoClient.getDatabase(MONGO_DATABASE)
                    .getCollection(COLLECTION_PREFIX + collectionCounter)
                    .drop();

        mongoClient.getDatabase(MONGO_DATABASE)
                    .createCollection(COLLECTION_PREFIX + collectionCounter);
    }

    private void startStreamReadFromMongoPipeline(final HazelcastInstance client, final int collectionCounter) {
        final BsonTimestamp startAt = MongoUtilities.localDateTimeToTimestamp(LocalDateTime.now());
        final String connectionString = mongoConnectionString;

        final StreamSource<Document> mongoSource = MongoSources
                .stream(() -> MongoClients.create(connectionString))
                .database(MONGO_DATABASE)
                .collection(COLLECTION_PREFIX + collectionCounter)
                .startAtOperationTime(startAt)
                .checkResourceExistence(NEVER)
                .build();

        final Pipeline fromMongo = Pipeline.create();
        fromMongo.readFrom(mongoSource)
                .withNativeTimestamps(0)
                .writeTo(Sinks.list(STREAM_SINK_LIST_NAME));

        final JobConfig jobConfig = new JobConfig();

        jobConfig.setName(STREAM_READ_FROM_PREFIX + collectionCounter);
        final Job job = client.getJet().newJob(fromMongo, jobConfig);
        assertJobStatusEventually(job);

    }

    private void executeWriteToMongoPipeline(final HazelcastInstance client, final int collectionCounter) {
        final String connectionString = mongoConnectionString;

        final Sink<Document> mongoSink = MongoSinks.builder(Document.class,
                        () -> MongoClients.create(connectionString))
                .into(MONGO_DATABASE, COLLECTION_PREFIX + collectionCounter)
                .identifyDocumentBy("_id", doc -> doc.get("_id"))
                .checkResourceExistence(NEVER)
                .build();

        final Pipeline toMongo = Pipeline.create();
        toMongo.readFrom(TestSources.items(inputItems))
                .map(docIndex -> new Document()
                        .append("docId", DOC_PREFIX + collectionCounter + DOC_COUNTER_PREFIX + docIndex))
                .rebalance()
                .writeTo(mongoSink);

        final JobConfig jobConfig = new JobConfig();
        jobConfig.setName(WRITE_PREFIX + collectionCounter);
        client.getJet().newJob(toMongo, jobConfig).join();
    }

    private void assertBatchResults(final HazelcastInstance client, final int collectionCounter) {
        assertResults(client.getList(BATCH_SINK_LIST_NAME), collectionCounter, "batch");
    }

    private void assertStreamResults(final HazelcastInstance client, final int collectionCounter) {
        for (int i = 0; i < STREAM_SINK_ASSERTION_ATTEMPTS; i++) {
            long actualTotalCount = client.getList(STREAM_SINK_LIST_NAME).size();
            if (itemCount == actualTotalCount) {
                return;
            }
            sleepMillis(STREAM_SINK_ASSERTION_SLEEP_MS);
        }

        assertResults(client.getList(STREAM_SINK_LIST_NAME), collectionCounter, "stream");
    }

    private void executeBatchReadFromMongoPipeline(final HazelcastInstance client, final int collectionCounter) {
        final String connectionString = mongoConnectionString;

        final BatchSource<Document> mongoSource = MongoSources
                .batch(() -> MongoClients.create(connectionString))
                .database(MONGO_DATABASE)
                .collection(COLLECTION_PREFIX + collectionCounter)
                .checkResourceExistence(NEVER)
                .build();

        final Pipeline fromMongo = Pipeline.create();
        fromMongo.readFrom(mongoSource)
                .writeTo(Sinks.list(BATCH_SINK_LIST_NAME));

        final JobConfig jobConfig = new JobConfig();
        jobConfig.setName(BATCH_READ_FROM_PREFIX + collectionCounter);
        client.getJet().newJob(fromMongo, jobConfig).join();
    }

    private void assertResults(final IList<Document> list, final int collectionCounter, final String type) {
        final Set<String> set = new HashSet<>();
        final String expected = DOC_PREFIX + collectionCounter + DOC_COUNTER_PREFIX;
        for (final Document item : list) {
            assertTrue(type + " does not contain expected part: " + item, item.getString("docId").contains(expected));
            set.add(item.getString("docId"));
        }

        final int lastElementNumber = itemCount - 1;
        try {
            assertEquals(itemCount, list.size());
            assertEquals(itemCount, set.size());
            assertTrue(set.contains(docId(collectionCounter, 0)));
            assertTrue(set.contains(docId(collectionCounter, lastElementNumber)));
        } catch (final Throwable ex) {
            logger.info("Printing content of incorrect list for " + type + ":");
            for (final Document item : list) {
                logger.info(item.toString());
            }
            throw ex;
        }
    }

}
