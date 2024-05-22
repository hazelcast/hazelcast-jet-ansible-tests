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

package com.hazelcast.jet.tests.mongo.stream;

import com.hazelcast.logging.ILogger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class MongoDocsProducer {
    private static final int PRINT_LOG_INSERT_ITEMS = 5_000;
    private final String mongoUrl;
    private final String database;
    private final String collectionName;
    private final ILogger logger;
    private final Thread producerThread;
    private volatile boolean running = true;
    private volatile long producedItems;


    public MongoDocsProducer(final String mongoUrl, final String database, final String collectionName, ILogger logger) {
        this.mongoUrl = mongoUrl;
        this.database = database;
        this.collectionName = collectionName;
        this.logger = logger;
        this.producerThread = new Thread(() -> uncheckRun(this::run));
    }

    private void run() {
        try (MongoClient client = MongoClients.create(mongoUrl)) {
            MongoCollection<Document> collection = client.getDatabase(database)
                    .getCollection(collectionName);
            long id = 0;
            while (running) {
                collection
                        .insertOne(new Document("docId", id++));
                producedItems = id;

                if (id % PRINT_LOG_INSERT_ITEMS == 0) {
                    logger.info(String.format("Inserted %d docs into %s collection)", id, collectionName));
                }
                sleepMillis(150);
            }
        } finally {
            logger.info(String.format("Total number of inserted docs into %s collection is %d",
                    collectionName,
                    producedItems));
        }
    }

    public void start() {
        producerThread.start();
    }

    public long stop() throws InterruptedException {
        running = false;
        producerThread.join();
        return producedItems;
    }

}
