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

package com.hazelcast.jet.tests.snapshot.jdbc;

import com.hazelcast.logging.ILogger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import javax.sql.DataSource;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.snapshot.jdbc.JdbcSinkTest.DataSourceSupplier.getDataSourceSupplier;
import static com.hazelcast.jet.tests.snapshot.jdbc.JdbcSinkTest.TABLE_PREFIX;

public class JdbcSinkVerifier {

    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_MS = 1000;
    private static final int ALLOWED_NO_INPUT_MS = 600000;
    private static final int QUEUE_SIZE_LIMIT = 20_000;
    private static final int PRINT_LOG_ITEMS = 10_000;

    private final String tableName;

    private final Thread consumerThread;
    private final String name;
    private final ILogger logger;
    private final String connectionUrl;

    private volatile boolean finished;
    private volatile Throwable error;
    private long counter;
    private final PriorityQueue<Integer> verificationQueue = new PriorityQueue<>();

    public JdbcSinkVerifier(String name, ILogger logger, String connectionUrl) {
        this.consumerThread = new Thread(() -> uncheckRun(this::run));
        this.name = name;
        this.tableName = (TABLE_PREFIX + name).replaceAll("-", "_");
        this.logger = logger;
        this.connectionUrl = connectionUrl;
    }

    private void run() throws Exception {
        long lastInputTime = System.currentTimeMillis();
        while (!finished) {
            try {
                List<Integer> ids = new ArrayList<>();
                try (Connection connection
                        = ((DataSource) getDataSourceSupplier(connectionUrl).get()).getConnection()) {
                    ResultSet resultSet = connection.createStatement()
                            .executeQuery("SELECT * FROM " + tableName);
                    while (resultSet.next()) {
                        ids.add(resultSet.getInt(1));
                        verificationQueue.add(resultSet.getInt(2));
                    }
                }

                long now = System.currentTimeMillis();
                if (ids.isEmpty()) {
                    if (now - lastInputTime > ALLOWED_NO_INPUT_MS) {
                        throw new AssertionError(
                                String.format("[%s] No new data was added during last %s", name, ALLOWED_NO_INPUT_MS));
                    }
                } else {
                    verifyQueue();
                    removeLoaded(ids);
                    lastInputTime = now;
                }
                Thread.sleep(SLEEP_AFTER_VERIFICATION_CYCLE_MS);
            } catch (Throwable e) {
                logger.severe("[" + name + "] Exception thrown during processing files.", e);
                error = e;
                finished = true;
            }
        }
    }

    private void verifyQueue() {
        // try to verify head of verification queue
        for (Integer peeked; (peeked = verificationQueue.peek()) != null;) {
            if (peeked > counter) {
                // the item might arrive later
                break;
            } else if (peeked == counter) {
                if (counter % PRINT_LOG_ITEMS == 0) {
                    logger.info(String.format("[%s] Processed correctly item %d", name, counter));
                }
                // correct head of queue
                verificationQueue.remove();
                counter++;
            } else if (peeked < counter) {
                // duplicate key
                throw new AssertionError(
                        String.format("Duplicate key %d, but counter was %d", peeked, counter));
            }
        }
        if (verificationQueue.size() >= QUEUE_SIZE_LIMIT) {
            throw new AssertionError(String.format("[%s] Queue size exceeded while waiting for the next "
                    + "item. Limit=%d, expected next=%d, next in queue: %s, %s, %s, %s, ...",
                    name, QUEUE_SIZE_LIMIT, counter, verificationQueue.poll(), verificationQueue.poll(),
                    verificationQueue.poll(), verificationQueue.poll()));
        }
    }

    private void removeLoaded(List<Integer> ids) throws Exception {
        try (Connection connection = ((DataSource) getDataSourceSupplier(connectionUrl).get()).getConnection()) {
            for (Integer id : ids) {
                connection.createStatement().execute("DELETE FROM " + tableName + " WHERE id=" + id);
            }
        }
    }

    void start() {
        consumerThread.start();
    }

    public void finish() throws InterruptedException {
        finished = true;
        consumerThread.join();
    }

    public void checkStatus() {
        if (error != null) {
            throw new RuntimeException(error);
        }
        if (finished) {
            throw new RuntimeException("[" + name + "] Verifier is not running");
        }
    }

}
