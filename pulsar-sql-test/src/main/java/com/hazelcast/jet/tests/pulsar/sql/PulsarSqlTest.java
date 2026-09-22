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

package com.hazelcast.jet.tests.pulsar.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.shaded.org.json.JSONObject;
import com.hazelcast.sql.SqlService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.lang.String.format;

/**
 * SQL-only soak test for the new Pulsar SQL connector, directly analogous to
 * {@code MongoSqlTest} but scoped to what the connector actually supports: unlike
 * {@code PulsarSqlConnector}, the Mongo SQL connector implements {@code updateProcessor}
 * and {@code deleteProcessor} and offers a {@code ChangeStream} object type, so
 * {@code MongoSqlTest} exercises UPDATE, DELETE and a long-lived change-stream mapping.
 * {@code PulsarSqlConnector} only implements {@code fullScanReader} (SELECT) and
 * {@code insertProcessor}/{@code sinkProcessor} (INSERT) - there is no SQL UPDATE or
 * DELETE support and no change-stream equivalent for Pulsar. This test is therefore
 * scoped to a {@code CREATE MAPPING} + {@code INSERT INTO} + {@code SELECT FROM}
 * round trip, cross-verified natively with a plain {@code PulsarClient} reader.
 */
public class PulsarSqlTest extends AbstractJetSoakTest {
    private static final String SELECT_ALL_FROM = "SELECT __key FROM ";
    private static final String DROP_MAPPING = "DROP MAPPING ";
    private static final int DEFAULT_ITEM_COUNT = 1_000;
    private static final int BATCH_ITEM_COUNT = 500;
    private static final int DEFAULT_TOPIC_PARTITIONS = 4;
    private static final int LOG_JOB_COUNT_THRESHOLD = 50;
    private static final int SLEEP_BETWEEN_READS_SECONDS = 2;
    private static final int ASSERTION_ATTEMPTS = 1200;
    private static final int ASSERTION_SLEEP_MS = 100;
    private static final int NATIVE_READ_TIMEOUT_SECONDS = 5;

    private static final String DATA_CONNECTION_NAME = "Pulsar";
    private static final String TOPIC_NAMESPACE = "persistent://public/default/";
    private static final String TOPIC_PREFIX = PulsarSqlTest.class.getSimpleName() + "_topic_";
    private static final String MAPPING_PREFIX = PulsarSqlTest.class.getSimpleName() + "_mapping_";
    private static final String DOC_PREFIX = "pulsar-sql-message-from-topic-";
    private static final String DOC_COUNTER_PREFIX = "-counter-";

    private String brokerUrl;
    private String httpServiceUrl;
    private int itemCount;
    private int topicPartitions;
    private transient SqlService sqlService;
    private transient PulsarAdmin pulsarAdmin;
    private transient PulsarClient pulsarClient;

    public static void main(final String[] args) throws Exception {
        new PulsarSqlTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) throws Exception {
        brokerUrl = "pulsar://" + property("pulsarIp", "127.0.0.1") + ":6650";
        httpServiceUrl = "http://" + property("pulsarIp", "127.0.0.1") + ":8080";
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        topicPartitions = propertyInt("topicPartitions", DEFAULT_TOPIC_PARTITIONS);
        sqlService = client.getSql();
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(httpServiceUrl).build();
        pulsarClient = PulsarClient.builder().serviceUrl(brokerUrl).build();
    }

    @Override
    public void test(final HazelcastInstance client, final String name) throws Exception {
        int jobCounter = 0;
        final long begin = System.currentTimeMillis();
        try {
            final String createDataConnection = "CREATE DATA CONNECTION"
                    + " " + DATA_CONNECTION_NAME
                    + " TYPE Pulsar SHARED"
                    + " OPTIONS ("
                    + "  'brokerUrl' = '" + brokerUrl + "',"
                    + "  'httpServiceUrl' = '" + httpServiceUrl + "',"
                    + "  'enableTransactions' = 'true'"
                    + " )";
            logger.info("Creating Pulsar data connection against " + brokerUrl);
            executeQueryWithNoErrorAssert(createDataConnection);
            logger.info("Pulsar data connection created");

            while (System.currentTimeMillis() - begin < durationInMillis) {
                final String topicName = TOPIC_PREFIX + jobCounter;
                final String mappingName = MAPPING_PREFIX + jobCounter;

                deleteTopicAndCreateNewOne(topicName);

                final String createMapping = "CREATE MAPPING " + mappingName
                        + " EXTERNAL NAME " + topicName + " ("
                        + " __key VARCHAR,"
                        + " docId VARCHAR,"
                        + " docIndex INT"
                        + " )"
                        + " DATA CONNECTION " + DATA_CONNECTION_NAME
                        + " OPTIONS ("
                        + " 'keyFormat' = 'varchar',"
                        + " 'valueFormat' = 'json-flat'"
                        + " )";
                executeQueryWithNoErrorAssert(createMapping);

                insertDataViaSqlService(mappingName, jobCounter);
                assertSqlResultsCountEventually(itemCount, SELECT_ALL_FROM + mappingName);
                assertMappingContentsViaSql(mappingName, jobCounter);
                assertTopicContentsViaNativeClient(topicName, jobCounter);

                executeQueryWithNoErrorAssert(DROP_MAPPING + mappingName);
                deleteTopic(topicName);

                logger.info("Job " + jobCounter + ": inserted and verified " + itemCount
                        + " items via SQL and native client (" + mappingName + ")");

                if (jobCounter % LOG_JOB_COUNT_THRESHOLD == 0) {
                    logger.info("Job count: " + jobCounter);
                }

                jobCounter++;
                sleepSeconds(SLEEP_BETWEEN_READS_SECONDS);
            }
        } finally {
            logger.info("Test finished with job count: " + jobCounter);
            if (pulsarClient != null) {
                pulsarClient.close();
            }
        }
    }

    @Override
    protected void teardown(final Throwable t) {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    private static String docId(final int jobCounter, final int docIndex) {
        return DOC_PREFIX + jobCounter + DOC_COUNTER_PREFIX + docIndex;
    }

    private void executeQueryWithNoErrorAssert(final String query, final Object... arguments) {
        try (SqlResult sqlResult = sqlService.execute(query, arguments)) {
            // updateCount() is deprecated for removal since 5.6 and now always returns 0 for DML/DDL,
            // so it can no longer be used to assert anything meaningful. isRowSet() is the still-supported
            // way to confirm this was a non-row-returning (DDL/DML) statement; a failed statement would
            // already have thrown out of execute(...) above.
            assertFalse(sqlResult.isRowSet());
        }
    }

    /**
     * Polls a plain (non-aggregate) SELECT until it returns the expected number of rows, counting
     * them by iterating the result client-side. Pulsar mappings are always registered as streaming
     * (unbounded) tables - see {@code PulsarTable}'s hardcoded {@code isStreaming=true} - so a
     * pushed-down {@code SELECT COUNT(*)} is rejected by Calcite with "Streaming aggregation is
     * supported only for window aggregation...".
     * <p>
     * Counting rows from a plain SELECT sidesteps that restriction, but the iteration itself has to
     * stop reading as soon as {@code expectedCount} rows have been seen: since the source is
     * genuinely unbounded, {@code Iterator#hasNext()} blocks waiting for the next message once the
     * topic is caught up, and one will never arrive after the producer side has finished inserting.
     * A plain {@code for}-each loop (as used here previously, and as {@link #assertMappingContentsViaSql}
     * still does below) never calls {@code hasNext()} again once the underlying iterable is
     * "exhausted" by definition for a bounded source - but this source never reports exhaustion, so
     * that loop hangs forever the first time it actually reaches the tail of the topic.
     */
    private void assertSqlResultsCountEventually(final long expectedCount, final String sql,
                                                 final Object... sqlArguments) {
        long count = -1;
        for (int i = 0; i < ASSERTION_ATTEMPTS; i++) {
            count = 0;
            try (SqlResult sqlResult = sqlService.execute(sql, sqlArguments)) {
                final Iterator<SqlRow> it = sqlResult.iterator();
                while (count < expectedCount && it.hasNext()) {
                    it.next();
                    count++;
                }
            }
            if (count == expectedCount) {
                return;
            }
            sleepMillis(ASSERTION_SLEEP_MS);
        }
        throw new AssertionError(format("Sql \" %s\" does not have expected count: %d. Current count : %d",
                sql, expectedCount, count));
    }

    private void insertDataViaSqlService(final String mappingName, final int jobCounter) {
        final String insertIntoPrefix = "INSERT INTO " + mappingName
                + " (__key, docId, docIndex) VALUES ";
        final StringBuilder query = new StringBuilder(insertIntoPrefix);
        for (int i = 0; i < itemCount; i++) {
            query.append(format("('%s','%s',%d)", "key-" + jobCounter + "-" + i, docId(jobCounter, i), i));
            if ((i + 1) % BATCH_ITEM_COUNT == 0) {
                executeQueryWithNoErrorAssert(query.toString());
                query.delete(0, query.length());
                if (i + 1 != itemCount) {
                    query.append(insertIntoPrefix);
                }
            } else {
                query.append(",");
            }
        }
        if (query.length() != 0) {
            executeQueryWithNoErrorAssert(query.toString());
        }
    }

    private void assertMappingContentsViaSql(final String mappingName, final int jobCounter) {
        // Same reasoning as assertSqlResultsCountEventually: this is an unbounded streaming source,
        // so the loop must stop pulling once it has itemCount rows rather than waiting for the
        // iterator to report it is exhausted, which it never will.
        final Set<String> docIds = new HashSet<>();
        try (SqlResult sqlResult = sqlService.execute("SELECT docId FROM " + mappingName)) {
            final Iterator<SqlRow> it = sqlResult.iterator();
            while (docIds.size() < itemCount && it.hasNext()) {
                docIds.add(it.next().getObject(0));
            }
        }
        assertEquals(itemCount, docIds.size());
        assertTrue(docIds.contains(docId(jobCounter, 0)));
        assertTrue(docIds.contains(docId(jobCounter, itemCount - 1)));
    }

    /**
     * Cross-verifies the mapping's SQL-visible content by reading the topic directly with a plain
     * {@link PulsarClient} {@link Reader}, bypassing SQL entirely.
     * <p>
     * Reads as raw {@code Schema.BYTES} rather than {@code Schema.JSON(...)}: the SQL connector's
     * {@code json-flat} writer publishes messages with no registered Pulsar schema (an "empty(BYTES)"
     * schema on the topic), and asking for a JSON reader schema on top of that makes the broker try
     * to add an incompatible JSON schema to an already-active BYTES-schema topic, which it rejects
     * with an {@code IncompatibleSchemaException}. Parsing the JSON body ourselves sidesteps that.
     * <p>
     * The topic is partitioned (see {@link #deleteTopicAndCreateNewOne}), and {@link Reader} - unlike
     * {@link org.apache.pulsar.client.api.Consumer} - cannot be pointed at a partitioned topic's
     * logical name directly ("Reader is not allowed to be used with a partitioned topic"); it only
     * reads a single partition, addressed as {@code <topic>-partition-<index>}. So this opens one
     * reader per partition in turn and pools the results together - insertion order across
     * partitions isn't guaranteed anyway (routing is by {@code __key}), and this test only cares
     * about the full set of docIds being present, not their order.
     */
    private void assertTopicContentsViaNativeClient(final String topicName, final int jobCounter)
            throws IOException {
        final Set<String> docIds = new HashSet<>();
        for (int partition = 0; partition < topicPartitions && docIds.size() < itemCount; partition++) {
            final String partitionTopicName = topicName + "-partition-" + partition;
            try (Reader<byte[]> reader = pulsarClient.newReader(Schema.BYTES)
                    .topic(partitionTopicName)
                    .startMessageId(MessageId.earliest)
                    .readerName("nativeVerify-" + jobCounter + "-p" + partition)
                    .create()) {
                while (docIds.size() < itemCount) {
                    final Message<byte[]> message = reader.readNext(NATIVE_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (message == null) {
                        break;
                    }
                    final JSONObject json = new JSONObject(new String(message.getValue(), StandardCharsets.UTF_8));
                    docIds.add(json.getString("docId"));
                }
            }
        }
        assertEquals(itemCount, docIds.size());
        assertTrue(docIds.contains(docId(jobCounter, 0)));
        assertTrue(docIds.contains(docId(jobCounter, itemCount - 1)));
    }

    private void deleteTopicAndCreateNewOne(final String topicName) throws PulsarAdminException {
        final String fullTopicName = TOPIC_NAMESPACE + topicName;
        deleteTopic(topicName);
        pulsarAdmin.topics().createPartitionedTopic(fullTopicName, topicPartitions);
    }

    /**
     * Deletes a (partitioned) topic, ignoring failures - this is also used defensively before
     * creating a topic in case a previous run left one behind. Every topic this test creates is
     * partitioned (see {@link #deleteTopicAndCreateNewOne}), so this always goes through
     * {@code deletePartitionedTopic} - the plain {@code delete} call rejects a partitioned topic
     * with "This is a Partitioned Topic, please try Partitioned-Topic-CLI to delete it".
     */
    private void deleteTopic(final String topicName) {
        final String fullTopicName = TOPIC_NAMESPACE + topicName;
        try {
            pulsarAdmin.topics().deletePartitionedTopic(fullTopicName, true);
        } catch (final PulsarAdminException e) {
            logger.info("Topic " + fullTopicName + " could not be deleted, ignoring: " + e.getMessage());
        }
    }

}
