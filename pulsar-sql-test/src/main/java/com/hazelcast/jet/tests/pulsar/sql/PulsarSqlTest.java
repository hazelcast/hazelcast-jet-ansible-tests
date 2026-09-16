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
import com.hazelcast.sql.SqlService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.HashSet;
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
    private static final String SELECT_COUNT_FROM = "SELECT COUNT(*) FROM ";
    private static final String DROP_MAPPING = "DROP MAPPING ";
    private static final int DEFAULT_ITEM_COUNT = 1_000;
    private static final int BATCH_ITEM_COUNT = 500;
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
            executeQueryWithNoErrorAssert(createDataConnection);

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

                assertSqlResultsCountEventually(itemCount, SELECT_COUNT_FROM + mappingName);
                assertMappingContentsViaSql(mappingName, jobCounter);
                assertTopicContentsViaNativeClient(topicName, jobCounter);

                executeQueryWithNoErrorAssert(DROP_MAPPING + mappingName);
                deleteTopic(topicName);

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

    private void assertSqlResultsCountEventually(final long expectedCount, final String sql,
                                                 final Object... sqlArguments) {
        Long count = null;
        for (int i = 0; i < ASSERTION_ATTEMPTS; i++) {
            try (SqlResult sqlResult = sqlService.execute(sql, sqlArguments)) {
                count = sqlResult.iterator().next().getObject(0);
                if (count.equals(expectedCount)) {
                    return;
                } else {
                    sleepMillis(ASSERTION_SLEEP_MS);
                }
            }
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
        final Set<String> docIds = new HashSet<>();
        try (SqlResult sqlResult = sqlService.execute("SELECT docId FROM " + mappingName)) {
            for (final SqlRow row : sqlResult) {
                docIds.add(row.getObject(0));
            }
        }
        assertEquals(itemCount, docIds.size());
        assertTrue(docIds.contains(docId(jobCounter, 0)));
        assertTrue(docIds.contains(docId(jobCounter, itemCount - 1)));
    }

    /**
     * Cross-verifies the mapping's SQL-visible content by reading the topic directly with a plain
     * {@link PulsarClient} {@link Reader}, bypassing SQL entirely.
     */
    private void assertTopicContentsViaNativeClient(final String topicName, final int jobCounter)
            throws IOException {
        final Set<String> docIds = new HashSet<>();
        try (Reader<PulsarSqlRow> reader = pulsarClient.newReader(Schema.JSON(PulsarSqlRow.class))
                .topic(topicName)
                .startMessageId(MessageId.earliest)
                .readerName("nativeVerify-" + jobCounter)
                .create()) {
            for (int i = 0; i < itemCount; i++) {
                final Message<PulsarSqlRow> message = reader.readNext(NATIVE_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (message == null) {
                    break;
                }
                docIds.add(message.getValue().docId());
            }
        }
        assertEquals(itemCount, docIds.size());
        assertTrue(docIds.contains(docId(jobCounter, 0)));
        assertTrue(docIds.contains(docId(jobCounter, itemCount - 1)));
    }

    private void deleteTopicAndCreateNewOne(final String topicName) throws PulsarAdminException {
        final String fullTopicName = TOPIC_NAMESPACE + topicName;
        try {
            pulsarAdmin.topics().delete(fullTopicName, true);
        } catch (final PulsarAdminException e) {
            logger.info("Topic " + fullTopicName + " did not exist yet, nothing to delete");
        }
        pulsarAdmin.topics().createNonPartitionedTopic(fullTopicName);
    }

    private void deleteTopic(final String topicName) {
        final String fullTopicName = TOPIC_NAMESPACE + topicName;
        try {
            pulsarAdmin.topics().delete(fullTopicName, true);
        } catch (final PulsarAdminException e) {
            logger.info("Topic " + fullTopicName + " could not be deleted, ignoring: " + e.getMessage());
        }
    }

}
