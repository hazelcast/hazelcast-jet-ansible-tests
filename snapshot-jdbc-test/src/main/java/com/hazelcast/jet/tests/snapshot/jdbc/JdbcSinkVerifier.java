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

package com.hazelcast.jet.tests.snapshot.jdbc;

import com.hazelcast.logging.ILogger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.snapshot.jdbc.JdbcSinkTest.DataSourceSupplier.getDataSourceSupplier;
import static com.hazelcast.jet.tests.snapshot.jdbc.JdbcSinkTest.TABLE_PREFIX;

public class JdbcSinkVerifier {

    private static final int LOCK_WAIT_TIMEOUT_SECONDS = 20;
    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_MS = 1_000;
    private static final int ALLOWED_NO_INPUT_MS = 600_000;
    private static final int QUEUE_SIZE_LIMIT = 20_000;
    private static final int PRINT_LOG_ITEMS = 10_000;
    private static final int SELECT_SIZE_LIMIT = 1_000;

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
        // lower the default lock wait timeout, in case of a deadlock, verifier's statements will
        // terminate sooner than Jet ones, allowing the latter ones to proceed and hopefully succeed
        connection.createStatement().execute("SET innodb_lock_wait_timeout = " + LOCK_WAIT_TIMEOUT_SECONDS);
        selectStatement = connection.prepareStatement("SELECT id, value FROM " + tableName +
                " ORDER BY id LIMIT " + SELECT_SIZE_LIMIT);
        deleteStatement = connection.prepareStatement("DELETE FROM " + tableName + " WHERE id=?");
    }

    private void run() {
        long lastInputTime = System.currentTimeMillis();
        while (!finished) {
            try (ResultSet resultSet = selectStatement.executeQuery()) {
                List<Integer> idList = new ArrayList<>();
                while (resultSet.next()) {
                    idList.add(resultSet.getInt(1));
                    verificationQueue.add(resultSet.getInt(2));
                }

                long now = System.currentTimeMillis();
                if (idList.isEmpty()) {
                    if (now - lastInputTime > ALLOWED_NO_INPUT_MS) {
                        throw new AssertionError(
                                String.format("[%s] No new data was added during last %s", name, ALLOWED_NO_INPUT_MS));
                    }
                } else {
                    verifyQueue();
                    removeLoaded(idList);
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

    private void removeLoaded(List<Integer> idList) throws Exception {
        deleteStatement.clearParameters();
        for (Integer id : idList) {
            deleteStatement.setInt(1, id);
            deleteStatement.addBatch();
        }
        int[] batchResults = deleteStatement.executeBatch();
        int deletedRowCount = Arrays.stream(batchResults).sum();
        if (deletedRowCount != idList.size()) {
            throw new IllegalStateException("Expected deleted row count: " + idList.size()
                    + ", actual: " + deletedRowCount);
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
