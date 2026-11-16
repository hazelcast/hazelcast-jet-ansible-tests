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
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.shaded.org.json.JSONObject;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.lang.String.format;

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
    private static final long SQL_READ_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

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
            assertFalse(sqlResult.isRowSet());
        }
    }

    private void assertSqlResultsCountEventually(final long expectedCount, final String sql,
                                                 final Object... sqlArguments) {
        long count = -1;
        for (int i = 0; i < ASSERTION_ATTEMPTS; i++) {
            count = 0;
            final SqlStatement statement = new SqlStatement(sql)
                    .setParameters(Arrays.asList(sqlArguments))
                    .setTimeoutMillis(SQL_READ_TIMEOUT_MS);
            try (SqlResult sqlResult = sqlService.execute(statement)) {
                final Iterator<SqlRow> it = sqlResult.iterator();
                while (count < expectedCount && it.hasNext()) {
                    it.next();
                    count++;
                }
            } catch (final HazelcastSqlException e) {
                // the statement timeout above cancelled a hasNext() call that was blocked waiting for a
                // row that hadn't arrived yet - fall through and retry
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
        if (!query.isEmpty()) {
            executeQueryWithNoErrorAssert(query.toString());
        }
    }

    private void assertMappingContentsViaSql(final String mappingName, final int jobCounter) {
        // Same reasoning as assertSqlResultsCountEventually: this is an unbounded streaming source,
        // so the loop must stop pulling once it has itemCount rows rather than waiting for the
        // iterator to report it is exhausted, which it never will. And, same as there, a genuinely
        // missing row still blocks the last hasNext() call forever, so the statement timeout below
        // forcefully cancels it instead - the resulting HazelcastSqlException just means docIds falls
        // short of itemCount, which the assertEquals below reports clearly instead of hanging.
        final Set<String> docIds = new HashSet<>();
        final SqlStatement statement = new SqlStatement("SELECT docId FROM " + mappingName)
                .setTimeoutMillis(SQL_READ_TIMEOUT_MS);
        try (SqlResult sqlResult = sqlService.execute(statement)) {
            final Iterator<SqlRow> it = sqlResult.iterator();
            while (docIds.size() < itemCount && it.hasNext()) {
                docIds.add(it.next().getObject(0));
            }
        } catch (final HazelcastSqlException e) {
            // see the comment above - the statement timeout cancelled a blocked hasNext() call
        }
        assertEquals(itemCount, docIds.size());
        assertTrue(docIds.contains(docId(jobCounter, 0)));
        assertTrue(docIds.contains(docId(jobCounter, itemCount - 1)));
    }

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

    private void deleteTopic(final String topicName) {
        final String fullTopicName = TOPIC_NAMESPACE + topicName;
        try {
            pulsarAdmin.topics().deletePartitionedTopic(fullTopicName, true);
        } catch (final PulsarAdminException e) {
            logger.info("Topic " + fullTopicName + " could not be deleted, ignoring: " + e.getMessage());
        }
    }

}
