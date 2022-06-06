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

package com.hazelcast.jet.tests.common.sql;

import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;

import java.util.Iterator;
import java.util.concurrent.*;

public class SqlRowProcessor {

    private final String sqlString;
    private final SqlService sqlService;

    private final ExecutorService threadPool;

    public SqlRowProcessor(String sqlString, SqlService sqlService, ExecutorService threadPool) {
        this.sqlString = sqlString;
        this.sqlService = sqlService;
        this.threadPool = threadPool;
    }

    public Future<SqlRow> runQueryAsyncUntilRowFetched(int noResultWaitIntervalSeconds, int noResultTimeoutSeconds) {
        CompletableFuture<SqlRow> completableFuture = new CompletableFuture<>();
        threadPool.submit(
                () -> {
                    SqlStatement statement = new SqlStatement(sqlString);
                    try {
                        int elapsedTimeIndicatorSeconds = 0;

                        SqlResult result = sqlService.execute(statement);
                        Iterator<SqlRow> iterator = result.iterator();

                        while (!iterator.hasNext()) {
                            if (elapsedTimeIndicatorSeconds < noResultTimeoutSeconds) {
                                result = sqlService.execute(statement);
                                iterator = result.iterator();

                                Util.sleepMillis(noResultWaitIntervalSeconds);
                                elapsedTimeIndicatorSeconds += noResultWaitIntervalSeconds;
                                System.out.println("No result from async executor at elapsed time" + elapsedTimeIndicatorSeconds + " for query: " + sqlString);
                            } else {
                                System.out.println("Non zero result did not arrive within specified timeout: " + noResultTimeoutSeconds + " for query: " + sqlString);
                                completableFuture.completeExceptionally(new RuntimeException("SQL query is still empty, stopping after timeout: " + noResultTimeoutSeconds));
                            }
                        }
                        System.out.println("Existing result from async executor arrived at elapsed time: " + elapsedTimeIndicatorSeconds + " for query: " + sqlString);
                        completableFuture.complete(iterator.next());

                    } catch (Throwable e) {
                        e.printStackTrace();
                        completableFuture.completeExceptionally(e);
                    }
                    return null;
                }
        );
        return completableFuture;
    }

    public SqlRow awaitQueryExecutionWithTimeout(Future<SqlRow> sqlRowFuture, int timeoutSeconds) {
        SqlRow sqlRow = null;

        try {
            sqlRow = sqlRowFuture.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("SQL Query failed or timed out for request: " + sqlString, e);
        }
        return sqlRow;
    }
}
