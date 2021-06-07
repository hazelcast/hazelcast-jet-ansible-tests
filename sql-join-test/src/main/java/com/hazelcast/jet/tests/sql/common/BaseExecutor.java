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

package com.hazelcast.jet.tests.sql.common;

import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

public class BaseExecutor {

    private static final long PROGRESS_PRINT_INTERVAL = TimeUnit.SECONDS.toMillis(5);
    private static final long MAXIMUM_TIME_BETWEEN_QUERIES = TimeUnit.SECONDS.toMillis(30);

    protected long lastCheckpoint = System.currentTimeMillis();
    protected long begin;
    protected long threshold;
    protected int currentQueryCount;
    private ILogger logger;

    public BaseExecutor(ILogger logger, long begin, long threshold) {
        this.logger = logger;
        this.begin = begin;
        this.threshold = threshold;
    }

    protected void printProgress(String joinType) {
        boolean toPrint = (System.currentTimeMillis() - lastCheckpoint) >= PROGRESS_PRINT_INTERVAL;
        if (toPrint) {
            logger.info(String.format("Time elapsed: %s. Read %d records from SQL stream with " + joinType,
                    Util.getTimeElapsed(begin), currentQueryCount));
        }
    }

    protected void verifyNotStuck() {
        long millisSinceLastCheckpoint = System.currentTimeMillis() - lastCheckpoint;
        boolean isStuck = millisSinceLastCheckpoint > (MAXIMUM_TIME_BETWEEN_QUERIES + threshold);
        Assert.assertFalse(String.format("It's been more than %d seconds since last SQL query. " +
                        "The test seems to be stuck", TimeUnit.MILLISECONDS.toSeconds(millisSinceLastCheckpoint)),
                isStuck);
    }

    protected void populateMap(IMap<Key, Pojo> map, int entries) {
        for (int i = 0; i < entries; i++) {
            map.put(new Key(i), new Pojo(i));
        }
    }
}
