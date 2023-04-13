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

import com.hazelcast.logging.ILogger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class MongoDocsProducer {
    private static final int PRINT_LOG_INSERT_ITEMS = 10_000;
    private final String mongoUrl;
    private final String database;
    private final String collection;
    private final ILogger logger;
    private final Thread producerThread;
    private volatile boolean running = true;
    private volatile long producedItems;


    public MongoDocsProducer(final String mongoUrl, final String database, final String collection, ILogger logger) {
        this.mongoUrl = mongoUrl;
        this.database = database;
        this.collection = collection;
        this.logger = logger;
        this.producerThread = new Thread(() -> uncheckRun(this::run));
    }

    private void run() {
        try (MongoClient client = MongoClients.create(mongoUrl)) {
            long id = 0;
            while (running) {
                client.getDatabase(database)
                        .getCollection(collection)
                        .insertOne(new Document("docId", id++));
                producedItems = id;

                if (id % PRINT_LOG_INSERT_ITEMS == 0) {
                    logger.info(String.format("Inserted %d docs into %s collection)", id, collection));
                }
                sleepMillis(50);
            }
        } finally {
            logger.info(String.format("Inserted %d docs into %s collection eventually", producedItems, collection));
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
