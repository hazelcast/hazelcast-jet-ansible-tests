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


package com.hazelcast.jet.tests.cdc.source;

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class MessageProducer {

    private static final int PRINT_LOG_INSERT_ITEMS = 1_000;

    private final int sleepMs;
    private ILogger logger;
    private String tableName;
    private final Thread producerThread;
    private final Connection connection;
    private final PreparedStatement insertStatement;
    private final PreparedStatement updateStatement;
    private final PreparedStatement deleteStatement;
    private volatile boolean running = true;
    private volatile int producedItems;

    MessageProducer(String connectionUrl, String tableName, int sleepMs, ILogger logger) throws SQLException {
        this.sleepMs = sleepMs;
        this.logger = logger;
        this.tableName = tableName;
        this.producerThread = new Thread(() -> Util.uncheckRun(this::run));
        connection = DriverManager.getConnection(connectionUrl, "root", "Soak-test,1");
        insertStatement = connection.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?)");
        updateStatement = connection.prepareStatement("UPDATE " + tableName + " SET value=? WHERE id=?");
        deleteStatement = connection.prepareStatement("DELETE FROM " + tableName + " WHERE id=?");
    }

    private void run() throws Exception {
        int id = 1;
        while (running) {
            insert(id);
            update(id);
            delete(id);
            id++;
        }
        producedItems = id;
    }

    public void start() {
        producerThread.start();
    }

    public int stop() throws Exception {
        running = false;
        producerThread.join();
        insertStatement.close();
        updateStatement.close();
        deleteStatement.close();
        connection.close();
        return producedItems;
    }

    private void insert(int id) throws SQLException {
        insertStatement.clearParameters();
        insertStatement.setInt(1, id);
        insertStatement.setInt(2, id);
        insertStatement.executeUpdate();
        if (id % PRINT_LOG_INSERT_ITEMS == 0) {
            logger.info(String.format("[%s] Inserted item with id %d", tableName, id));
        }
        sleepMillis(sleepMs);
    }

    private void update(int id) throws SQLException {
        updateStatement.clearParameters();
        updateStatement.setInt(1, id + 1);
        updateStatement.setInt(2, id);
        updateStatement.executeUpdate();
        sleepMillis(sleepMs);
    }

    private void delete(int id) throws SQLException {
        deleteStatement.clearParameters();
        deleteStatement.setInt(1, id);
        deleteStatement.executeUpdate();
        sleepMillis(sleepMs);
    }
}
