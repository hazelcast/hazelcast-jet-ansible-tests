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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.PriorityQueue;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.snapshot.jdbc.JdbcSinkTest.DataSourceSupplier.getDataSourceSupplier;
import static com.hazelcast.jet.tests.snapshot.jdbc.JdbcSinkTest.TABLE_PREFIX;

public class JdbcSinkVerifier {

    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_MS = 1000;
    private static final int ALLOWED_NO_INPUT_MS = 600000;
    private static final int QUEUE_SIZE_LIMIT = 20_000;
    private static final int PRINT_LOG_ITEMS = 10_000;
    private static final int SELECT_SIZE_LIMIT = 1000;

    private final Thread consumerThread;
    private final String name;
    private final ILogger logger;
    private final PriorityQueue<Integer> verificationQueue = new PriorityQueue<>();
    private final Connection connection;
    private final PreparedStatement selectStatement;
    private final PreparedStatement deleteStatement;
    private volatile boolean finished;
    private volatile Throwable error;
    private long counter;

    public JdbcSinkVerifier(String name, ILogger logger, String connectionUrl) throws SQLException {
        this.consumerThread = new Thread(() -> uncheckRun(this::run));
        this.name = name;
        this.logger = logger;

        String tableName = TABLE_PREFIX + name.replaceAll("-", "_");

        connection = getDataSourceSupplier(connectionUrl).get().getConnection();
        selectStatement = connection.prepareStatement("SELECT * FROM " + tableName +
                " ORDER BY id LIMIT " + SELECT_SIZE_LIMIT);
        deleteStatement = connection.prepareStatement("DELETE FROM " + tableName + " WHERE id <= ?");
    }

    private void run() {
        long lastInputTime = System.currentTimeMillis();
        int lastId = -1;
        while (!finished) {
            try {
                int rowCount = 0;
                ResultSet resultSet = selectStatement.executeQuery();
                while (resultSet.next()) {
                    rowCount++;
                    lastId = resultSet.getInt(1);
                    verificationQueue.add(resultSet.getInt(2));
                }

                long now = System.currentTimeMillis();
                if (rowCount == 0) {
                    if (now - lastInputTime > ALLOWED_NO_INPUT_MS) {
                        throw new AssertionError(
                                String.format("[%s] No new data was added during last %s", name, ALLOWED_NO_INPUT_MS));
                    }
                } else {
                    verifyQueue();
                    removeLoaded(lastId, rowCount);
                    lastInputTime = now;
                }
                sleepMillis(SLEEP_AFTER_VERIFICATION_CYCLE_MS);
            } catch (Throwable e) {
                logger.severe("[" + name + "] Exception thrown during processing files.", e);
                error = e;
                finished = true;
            }
        }
    }

    private void verifyQueue() {
        // try to verify head of verification queue
        for (Integer peeked; (peeked = verificationQueue.peek()) != null; ) {
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

    private void removeLoaded(int lastId, int rowCount) throws Exception {
        deleteStatement.clearParameters();
        deleteStatement.setInt(1, lastId);
        int deletedRowCount = deleteStatement.executeUpdate();
        if (deletedRowCount != rowCount) {
            throw new IllegalStateException("Expected deleted row count: " + rowCount + ", actual: " + deletedRowCount);
        }
    }

    void start() {
        consumerThread.start();
    }

    public void finish() throws Exception {
        finished = true;
        consumerThread.join();
        selectStatement.close();
        deleteStatement.close();
        connection.close();
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
