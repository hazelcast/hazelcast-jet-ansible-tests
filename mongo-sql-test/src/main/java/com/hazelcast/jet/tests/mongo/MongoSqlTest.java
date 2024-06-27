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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.ValidationOptions;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.concurrent.TimeUnit.MINUTES;

public class MongoSqlTest extends AbstractJetSoakTest {
    public static final String SELECT_COUNT_FROM = "SELECT COUNT(*) FROM ";
    public static final String WHERE_UPDATED_AND_SOURCE_LIKE = " WHERE updated = ? AND source LIKE ?";
    public static final String WHERE_SOURCE_LIKE = " WHERE source LIKE ?";
    public static final String DROP_MAPPING = "DROP MAPPING ";
    public static final String DROP_JOB = "DROP JOB ";
    private static final int DEFAULT_ITEM_COUNT = 1_000;
    private static final int BATCH_ITEM_COUNT = 500;
    private static final int LOG_JOB_COUNT_THRESHOLD = 50;
    private static final int SLEEP_BETWEEN_READS_SECONDS = 2;
    private static final int ASSERTION_ATTEMPTS = 1200;
    private static final int ASSERTION_SLEEP_MS = 100;
    private static final String MONGO_DATABASE = MongoSqlTest.class.getSimpleName();
    private static final String SOURCE_COLLECTION_PREFIX = "source_collection_";
    private static final String SINK_COLLECTION_PREFIX = "sink_collection_";
    private static final String DOC_PREFIX = "mongo-document-from-collection-";
    private static final String DOC_COUNTER_PREFIX = "-counter-";
    private static final String LONG_LIVED_COLLECTION = "long_lived_collection";
    private String mongoConnectionString;
    private int itemCount;
    private MongoClient mongoClient;
    private SqlService sqlService;

    public static void main(final String[] args) throws Exception {
        new MongoSqlTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) {
        mongoConnectionString = "mongodb://" + property("mongoIp", "127.0.0.1") + ":27017";
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoConnectionString))
                .applyToConnectionPoolSettings(builder -> builder
                        .minSize(10)
                        .maxConnectionIdleTime(1, MINUTES)).build();
        mongoClient = MongoClients.create(mongoClientSettings);
        sqlService = client.getSql();
    }

    @Override
    public void test(final HazelcastInstance client, final String name) {
        int jobCounter = 0;
        final long begin = System.currentTimeMillis();
        dropMongoDatabase();
        try {
            final String createDataConnection = "CREATE DATA CONNECTION"
                    + " Mongo"
                    + " TYPE Mongo SHARED"
                    + " OPTIONS ( "
                    + "  'connectionString' = '" + mongoConnectionString + "',"
                    + "  'database' = '" + MONGO_DATABASE + "'"
                    + "  )";
            executeQueryWithNoErrorAssert(createDataConnection);

            //Long-lived mapping
            createLongLivedCollection(LONG_LIVED_COLLECTION);

            final String createLongLivedCollectionMapping = "CREATE MAPPING "
                    + LONG_LIVED_COLLECTION
                    + " EXTERNAL NAME " + MONGO_DATABASE + "." + LONG_LIVED_COLLECTION + " ("
                    + " id OBJECT external name _id,"
                    + " jobCounter INTEGER,"
                    + " jobFinishedAt VARCHAR,"
                    + " source VARCHAR"
                    + " ) "
                    + " DATA CONNECTION Mongo"
                    + " OBJECT TYPE Collection "
                    + " OPTIONS ( "
                    + " )";
            executeQueryWithNoErrorAssert(createLongLivedCollectionMapping);

            //Iterations part
            while (System.currentTimeMillis() - begin < durationInMillis) {
                final String sourceCollectionName = SOURCE_COLLECTION_PREFIX + jobCounter;
                final String sinkCollectionName = SINK_COLLECTION_PREFIX + jobCounter;
                final String startAt = Instant.now().atZone(ZoneOffset.UTC)
                        .with(ChronoField.MILLI_OF_SECOND, 0).format(DateTimeFormatter.ISO_INSTANT);


                createCollections(sourceCollectionName, sinkCollectionName);

                final String sourceMappingSql = "CREATE MAPPING "
                        + sourceCollectionName
                        + " EXTERNAL NAME " + MONGO_DATABASE + "." + sourceCollectionName + " ("
                        + " id OBJECT external name _id,"
                        + " collectionIndex INTEGER,"
                        + " docIndex INTEGER,"
                        + " docId VARCHAR,"
                        + " updated BOOLEAN,"
                        + " source VARCHAR"
                        + " ) "
                        + " DATA CONNECTION Mongo"
                        + " OBJECT TYPE ChangeStream "
                        + " OPTIONS ( "
                        + "   'startAt' = '" + startAt + "' "
                        + "   )";
                executeQueryWithNoErrorAssert(sourceMappingSql);

                final String sinkMappingSql = "CREATE MAPPING "
                        + sinkCollectionName
                        + " EXTERNAL NAME " + MONGO_DATABASE + "." + sinkCollectionName + " ("
                        + " id OBJECT external name _id,"
                        + " collectionIndex INTEGER,"
                        + " docIndex INTEGER,"
                        + " docId VARCHAR,"
                        + " updated BOOLEAN,"
                        + " source VARCHAR"
                        + " ) "
                        + " DATA CONNECTION Mongo"
                        + " OBJECT TYPE Collection "
                        + " OPTIONS ( "
                        + " )";
                executeQueryWithNoErrorAssert(sinkMappingSql);

                //Create job which writes from source into sink
                final String jobName = sourceCollectionName + "_into_" + sinkCollectionName;
                final String createJobWhichWritesFromSourceIntoSink = "CREATE JOB " + jobName
                        + " OPTIONS ("
                        + "    'processingGuarantee' = 'exactlyOnce',"
                        + "    'snapshotIntervalMillis' = '5000'"
                        + " ) AS "
                        + " INSERT INTO " + sinkCollectionName
                        + " SELECT * FROM " + sourceCollectionName;
                executeQueryWithNoErrorAssert(createJobWhichWritesFromSourceIntoSink);
                assertJobStatusEventually(client.getJet().getJob(jobName));

                //Insert
                insertDataViaMongoClientAndVerify(jobCounter, sourceCollectionName, sinkCollectionName);
                assertCollectionsAreEqual(sourceCollectionName, sinkCollectionName);
                insertDataViaSqlAndVerify(jobCounter, sourceCollectionName, sinkCollectionName);

                //Update
                updateDataViaMongoClientAndVerify(sinkCollectionName);
                updateDataViaSqlAndVerify(sinkCollectionName);

                //Delete
                deleteViaMongoClientAndVerify(sinkCollectionName);
                deleteOneViaSqlAndVerify(sinkCollectionName);

                //Cleanup
                dropMappingsAndCancelJob(sourceCollectionName, sinkCollectionName, jobName);
                deleteCollection(sourceCollectionName);
                deleteCollection(sinkCollectionName);

                jobCounter++;

                //Long-lived mapping verify
                insertDataIntoLongLivedCollectionAndVerify(jobCounter);

                if (jobCounter % LOG_JOB_COUNT_THRESHOLD == 0) {
                    logger.info("Job count: " + jobCounter);
                }
                sleepSeconds(SLEEP_BETWEEN_READS_SECONDS);
            }
        } finally {
            logger.info("Test finished with job count: " + jobCounter);
            mongoClient.close();
        }
    }

    private void dropMongoDatabase() {
        mongoClient.getDatabase(MONGO_DATABASE).drop();
    }

    private void executeQueryWithNoErrorAssert(final String query, final Object... arguments) {
        try (SqlResult sqlResult = sqlService.execute(query, arguments)) {
            assertEquals(0, sqlResult.updateCount());
        }
    }

    private void createLongLivedCollection(final String collection) {
        final String schema = "{"
                + "  $jsonSchema: {"
                + "    bsonType: 'object',"
                + "    required: ["
                + "      '_id','jobCounter','jobFinishedAt'"
                + "    ],"
                + "    properties: {"
                + "      _id: {"
                + "        bsonType: 'objectId',"
                + "        description: 'must be a ObjectId and is required'"
                + "      },"
                + "      jobCounter: {"
                + "        bsonType: 'int',"
                + "        description: 'must be a int and is required'"
                + "      },"
                + "      jobFinishedAt: {"
                + "        bsonType: 'string',"
                + "        description: 'must be a string and is required'"
                + "      }, "
                + "      source: {"
                + "        bsonType: 'string',"
                + "        description: 'must be a string and is required'"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        createNewCollectionWithSchema(collection, schema);
    }

    private void createCollections(final String... collectionNames) {
        for (String collectionName : collectionNames) {
            createCollectionWithDefaultSchema(collectionName);
        }
    }

    private static void assertJobStatusEventually(final Job job) {
        for (int i = 0; i < ASSERTION_ATTEMPTS; i++) {
            if (job.getStatus().equals(RUNNING)) {
                return;
            } else {
                sleepMillis(ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError(format("Job %s does not have expected status: %s. Job status: %s",
                job.getName(), RUNNING, job.getStatus()));
    }

    private void insertDataViaMongoClientAndVerify(final int jobCounter, final String sourceCollectionName,
                                                   final String sinkCollectionName) {
        insertDataViaMongoClient(sourceCollectionName, jobCounter);
        assertSqlResultsCountEventually(itemCount, SELECT_COUNT_FROM + sinkCollectionName);
    }

    private void assertCollectionsAreEqual(final String sourceCollectionName, final String sinkCollectionName) {
        final ArrayList<Document> sourceDocuments = new ArrayList<>();
        final MongoCollection<Document> sourceCollection = mongoClient.getDatabase(MONGO_DATABASE)
                .getCollection(sourceCollectionName);
        sourceCollection.find().into(sourceDocuments);

        final ArrayList<Document> sinkDocuments = new ArrayList<>();
        final MongoCollection<Document> sinkCollection = mongoClient.getDatabase(MONGO_DATABASE)
                .getCollection(sinkCollectionName);
        sinkCollection.find().into(sinkDocuments);

        assertEquals(sourceDocuments.size(), sinkDocuments.size());

        sourceDocuments.forEach(sourceDoc -> {
                    final ObjectId sourceDocId = sourceDoc.getObjectId("_id");
                    final Document sinkDoc = sinkDocuments.stream()
                            .filter(doc -> sourceDocId.equals(doc.getObjectId("_id")))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException(format("Source document from "
                                            + "collection %s with id: %s was not found in sink collection : %s",
                                    sourceCollection, sourceDocId, sinkCollection)));
                    assertTrue("Documents are not equal!\n Source document: \n" + sourceDoc.toJson()
                            + "\nSink document:\n" + sinkDoc.toJson(), sourceDoc.equals(sinkDoc));
                }
        );

    }

    private void insertDataViaSqlAndVerify(final int jobCounter, final String sourceCollectionName,
                                           final String sinkCollectionName) {
        insertDataViaSqlService(sinkCollectionName, jobCounter);

        final long sinkCountDocs = mongoClient.getDatabase(MONGO_DATABASE).getCollection(sinkCollectionName)
                .countDocuments();
        final long sourceCountDocs = mongoClient.getDatabase(MONGO_DATABASE).getCollection(sourceCollectionName)
                .countDocuments();

        assertEquals(itemCount, sourceCountDocs);
        assertEquals(itemCount * 2L, sinkCountDocs);

        assertSqlResultsCountEventually(itemCount, SELECT_COUNT_FROM + sinkCollectionName
                + WHERE_UPDATED_AND_SOURCE_LIKE, false, "sql");
    }

    private void updateDataViaMongoClientAndVerify(final String sinkCollectionName) {
        mongoClient.getDatabase(MONGO_DATABASE).getCollection(sinkCollectionName)
                .updateMany(new Document().append("source", "mongoClient"), Updates.set("updated", true));

        assertSqlResultsCountEventually(itemCount, SELECT_COUNT_FROM + sinkCollectionName
                + WHERE_UPDATED_AND_SOURCE_LIKE, true, "mongoClient");
    }

    private void updateDataViaSqlAndVerify(final String sinkCollectionName) {
        executeQueryWithNoErrorAssert(format("UPDATE %s SET updated = ? WHERE source LIKE ?", sinkCollectionName),
                true, "sql");

        assertSqlResultsCountEventually(itemCount, SELECT_COUNT_FROM + sinkCollectionName
                + WHERE_UPDATED_AND_SOURCE_LIKE, true, "sql");
    }

    private void deleteViaMongoClientAndVerify(final String sinkCollectionName) {
        mongoClient.getDatabase(MONGO_DATABASE).getCollection(sinkCollectionName)
                .deleteMany(new Document().append("source", "mongoClient"));
        assertSqlResultsCountEventually(itemCount, SELECT_COUNT_FROM + sinkCollectionName);
    }

    private void deleteOneViaSqlAndVerify(final String sinkCollectionName) {
        final MongoCollection<Document> collection = mongoClient.getDatabase(MONGO_DATABASE)
                .getCollection(sinkCollectionName);
        try (MongoCursor<Document> iterator = collection.find().iterator()) {
            final Document firstDoc = iterator.next();
            final ObjectId id = firstDoc.getObjectId("_id");
            final long countBeforeDelete = collection.countDocuments();
            executeQueryWithNoErrorAssert(format("DELETE FROM %s WHERE id = ?", sinkCollectionName), id);
            final long countAfterDelete = collection.countDocuments();
            assertEquals(countBeforeDelete - 1, countAfterDelete);
        }
    }

    private void dropMappingsAndCancelJob(final String sourceCollectionName, final String sinkCollectionName,
                                          final String jobName) {
        executeQueryWithNoErrorAssert(DROP_MAPPING + sinkCollectionName);
        executeQueryWithNoErrorAssert(DROP_MAPPING + sourceCollectionName);
        executeQueryWithNoErrorAssert(DROP_JOB + jobName);
    }

    private void deleteCollection(final String collection) {
        mongoClient.getDatabase(MONGO_DATABASE)
                .getCollection(collection)
                .drop();
    }

    private void insertDataIntoLongLivedCollectionAndVerify(int jobCounter) {
        String jobFinishedAt = LocalDateTime.now().format(ISO_DATE_TIME);
        insertJobSummaryIntoLongLivedCollectionViaSql(jobCounter, jobFinishedAt);
        insertJobSummaryIntoLongLivedCollectionViaMongo(mongoClient, jobCounter, jobFinishedAt);
        assertSqlResultsCountEventually(jobCounter, SELECT_COUNT_FROM + LONG_LIVED_COLLECTION
                + WHERE_SOURCE_LIKE, "mongo");
        assertSqlResultsCountEventually(jobCounter, SELECT_COUNT_FROM + LONG_LIVED_COLLECTION
                + WHERE_SOURCE_LIKE, "sql");
    }

    private void createNewCollectionWithSchema(final String collection, String schema) {
        mongoClient.getDatabase(MONGO_DATABASE)
                .createCollection(collection, new CreateCollectionOptions()
                        .validationOptions(new ValidationOptions().validator(Document.parse(schema))));
    }

    private void createCollectionWithDefaultSchema(final String collection) {
        final String schema = "{"
                + "  $jsonSchema: {"
                + "    bsonType: 'object',"
                + "    required: ["
                + "      '_id','collectionIndex','docIndex','docId','updated'"
                + "    ],"
                + "    properties: {"
                + "      _id: {"
                + "        bsonType: 'objectId',"
                + "        description: 'must be a ObjectId and is required'"
                + "      },"
                + "      collectionIndex: {"
                + "        bsonType: 'int',"
                + "        description: 'must be a int and is required'"
                + "      },"
                + "      docIndex: {"
                + "        bsonType: 'int',"
                + "        description: 'must be a int and is required'"
                + "      },"
                + "      docId: {"
                + "        bsonType: 'string',"
                + "        description: 'must be a string and is required'"
                + "      }, "
                + "      updated: {"
                + "        bsonType: 'bool',"
                + "        description: 'must be a bool and is required'"
                + "      }, "
                + "      source: {"
                + "        bsonType: 'string',"
                + "        description: 'must be a string and is required'"
                + "      } "
                + "    }"
                + "  }"
                + "}";
        createNewCollectionWithSchema(collection, schema);
    }

    private void insertDataViaMongoClient(final String collectionName, final int collectionCounter) {
        final MongoCollection<Document> collection = mongoClient.getDatabase(MONGO_DATABASE)
                .getCollection(collectionName);
        final List<InsertOneModel<Document>> batchList = new ArrayList<>();
        for (int i = 1; i <= itemCount; i++) {
            batchList.add(new InsertOneModel<>(
                    new Document()
                            .append("docId", DOC_PREFIX + collectionCounter + DOC_COUNTER_PREFIX + i)
                            .append("collectionIndex", collectionCounter)
                            .append("docIndex", i)
                            .append("updated", false)
                            .append("source", "mongoClient")
            ));

            if (i % BATCH_ITEM_COUNT == 0) {
                collection.bulkWrite(batchList, new BulkWriteOptions().ordered(false));
                batchList.clear();
            }
        }
        if (!batchList.isEmpty()) {
            collection.bulkWrite(batchList, new BulkWriteOptions().ordered(false));
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

    private void insertDataViaSqlService(final String sourceCollectionName, final int jobCounter) {
        final String insertIntoPrefix = "INSERT INTO " + sourceCollectionName
                + " (collectionIndex, docIndex, docId, updated, source) VALUES ";
        final StringBuilder query = new StringBuilder(insertIntoPrefix);
        for (int i = 1; i <= itemCount; i++) {
            query.append(format("( %d, %d, '%s%d%s%d',%s,'sql')",
                    jobCounter, i, DOC_PREFIX, jobCounter, DOC_COUNTER_PREFIX, i, false));
            if (i % BATCH_ITEM_COUNT == 0) {
                executeQueryWithNoErrorAssert(query.toString());
                query.delete(0, query.length());
                if (i != itemCount) {
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

    private void insertJobSummaryIntoLongLivedCollectionViaSql(int jobCounter, String jobFinishedAt) {
        executeQueryWithNoErrorAssert("INSERT INTO " + LONG_LIVED_COLLECTION +
                        " (jobCounter, jobFinishedAt, source) VALUES (?, ?, ?)",
                jobCounter, jobFinishedAt, "sql");
    }

    private void insertJobSummaryIntoLongLivedCollectionViaMongo(MongoClient mongoClient, int jobCounter,
                                                                 String jobFinishedAt) {
        mongoClient.getDatabase(MONGO_DATABASE).getCollection(LONG_LIVED_COLLECTION)
                .insertOne(new Document()
                        .append("jobCounter", jobCounter)
                        .append("source", "mongo")
                        .append("jobFinishedAt", jobFinishedAt));
    }

    @Override
    protected void teardown(final Throwable t) {
    }

}
