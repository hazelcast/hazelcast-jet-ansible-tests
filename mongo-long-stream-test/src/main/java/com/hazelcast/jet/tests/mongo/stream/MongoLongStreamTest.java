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

package com.hazelcast.jet.tests.mongo.stream;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.mongodb.MongoSources;
import com.hazelcast.jet.mongodb.impl.MongoUtilities;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.logging.ILogger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.util.Map;
import java.util.Optional;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.mongo.stream.MongoLongStreamTest.MongoClientSupplier.getMongoClient;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MongoLongStreamTest extends AbstractSoakTest {
    private static final String MONGO_DATABASE = MongoLongStreamTest.class.getSimpleName();
    private static final int ASSERTION_RETRY_COUNT = 60;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int ASSERTION_ATTEMPTS = 1200;
    private static final int ASSERTION_SLEEP_MS = 100;

    private String mongoConnectionString;
    private int snapshotIntervalMs;

    public static void main(final String[] args) throws Exception {
        new MongoLongStreamTest().run(args);
    }

    private static void assertJobStatusEventually(final Job job) {
        for (int i = 0; i < ASSERTION_ATTEMPTS; i++) {
            if (job.getStatus().equals(RUNNING)) {
                return;
            } else {
                sleepMillis(ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError("Job " + job.getName() + " does not have expected status: " + RUNNING
                + ". Job status: " + job.getStatus());
    }

    private static long getNumberOfCurrentlyProcessedDocs(final HazelcastInstance client, final String clusterName) {
        final Map<String, Long> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_DOCS_MAP_NAME);
        return Optional.ofNullable(latestCounterMap.get(clusterName)).orElse(0L);
    }

    private static void assertCountEventually(final HazelcastInstance client, final long expectedTotalCount,
                                              final ILogger logger, final String clusterName) throws Exception {
        final Map<String, Long> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_DOCS_MAP_NAME);
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            final long actualTotalCount = latestCounterMap.get(clusterName);
            if (expectedTotalCount == actualTotalCount) {
                return;
            }
            log(logger, "expected: " + expectedTotalCount + ", actual: " + actualTotalCount, clusterName);
            SECONDS.sleep(1);
        }
        final long actualTotalCount = latestCounterMap.get(clusterName);
        assertEquals(expectedTotalCount, actualTotalCount);
    }

    private static void log(final ILogger logger, final String message, final String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

    @Override
    public void init(final HazelcastInstance client) {
        mongoConnectionString = "mongodb://" + property("mongoIp", "127.0.0.1") + ":27017";
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(final HazelcastInstance client, final String clusterName) throws Exception {
        final long begin = System.currentTimeMillis();

        final StreamSource<Document> mongoSource = MongoSources
                .stream(clusterName + "_mongo-long-stream", getMongoClient(mongoConnectionString))
                .database(MONGO_DATABASE)
                .collection(clusterName)
                .startAtOperationTime(MongoUtilities.bsonTimestampFromTimeMillis(begin))
                .build();

        final Pipeline fromMongo = Pipeline.create();
        fromMongo.readFrom(mongoSource)
                .withNativeTimestamps(0)
                .map(doc -> doc.getLong("docId"))
                .writeTo(VerificationProcessor.sink(clusterName));

        final JobConfig jobConfig = new JobConfig();

        if (clusterName.startsWith(DYNAMIC_CLUSTER)) {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(EXACTLY_ONCE);
        } else {
            jobConfig.addClass(VerificationProcessor.class, MongoClientSupplier.class);
        }

        jobConfig.setName(clusterName + "_" + MONGO_DATABASE);

        final Job job = client.getJet().newJob(fromMongo, jobConfig);
        assertJobStatusEventually(job);

        final MongoDocsProducer producer = new MongoDocsProducer(mongoConnectionString, MONGO_DATABASE, clusterName);
        producer.start();

        final long expectedTotalCount;
        long lastlyProcessed = -1;
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(job) == FAILED) {
                    job.join();
                } else {
                    final long currentlyProcessedDocs = getNumberOfCurrentlyProcessedDocs(client, clusterName);

                    if (currentlyProcessedDocs == lastlyProcessed) {
                        log(logger, "Nothing was processed in last 1 minute, current counter:"
                                + currentlyProcessedDocs, clusterName);
                    }
                    lastlyProcessed = currentlyProcessedDocs;
                }
                sleepMinutes(1);
            }
        } finally {
            expectedTotalCount = producer.stop();
        }

        log(logger, "Producer stopped, expectedTotalCount: " + expectedTotalCount, clusterName);
        assertCountEventually(client, expectedTotalCount, logger, clusterName);
        job.cancel();
        log(logger, "Job completed", clusterName);

    }

    @Override
    protected void teardown(final Throwable t) {
    }

     static final class MongoClientSupplier {

        private MongoClientSupplier() {
        }

        static SupplierEx<MongoClient> getMongoClient(final String mongoConnectionString) {
            return () -> MongoClients.create(mongoConnectionString);
        }
    }

}
