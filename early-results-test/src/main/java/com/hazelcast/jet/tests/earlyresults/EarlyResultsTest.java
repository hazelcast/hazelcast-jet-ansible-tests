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

package com.hazelcast.jet.tests.earlyresults;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.logging.ILogger;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;

public class EarlyResultsTest extends AbstractJetSoakTest {

    private static final int ONE_THOUSAND = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 100;
    private static final int DEFAULT_TRADE_PER_SECOND = 20;

    private int windowSize;
    private int tradePerSecond;
    private long earlyResultsPeriod;

    public static void main(String[] args) throws Exception {
        new EarlyResultsTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        tradePerSecond = propertyInt("tradePerSecond", DEFAULT_TRADE_PER_SECOND);
        earlyResultsPeriod = windowSize * ONE_THOUSAND / tradePerSecond / 3;
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        Job job = client.getJet().newJob(pipeline(), jobConfig);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(job) == FAILED) {
                job.join();
            }
            sleepMinutes(1);
        }

        job.cancel();
    }

    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();

        int windowSizeLocal = windowSize;
        Sink<KeyedWindowResult<String, Long>> verificationSink = SinkBuilder
                .sinkBuilder("verification", c -> new VerificationContext(c.logger(), windowSizeLocal))
                .receiveFn(VerificationContext::verify)
                .build();

        StreamStage<Map.Entry<String, Long>> sourceStage = p.readFrom(TradeGenerator.tradeSource(tradePerSecond))
                .withNativeTimestamps(0)
                .setName("Stream from EarlyResult-TradeGenerator");

        sourceStage.groupingKey(Map.Entry::getKey)
                .window(WindowDefinition.tumbling(windowSize).setEarlyResultsPeriod(earlyResultsPeriod))
                .aggregate(AggregateOperations.counting())
                .writeTo(verificationSink);

        return p;
    }

    @Override
    protected void teardown(Throwable t) {
    }

    static class VerificationContext {

        private final int windowSize;
        private final ILogger logger;
        private final Map<String, TickerWindow> tickerMap;

        VerificationContext(ILogger logger, int windowSize) {
            this.logger = logger;
            this.windowSize = windowSize;
            this.tickerMap = new HashMap<>();
        }

        void verify(KeyedWindowResult<String, Long> result) {
            TickerWindow tickerWindow = tickerMap.computeIfAbsent(result.getKey(), k -> new TickerWindow());

            assertTrue("Received a result for the previous window: " + result,
                    result.start() >= tickerWindow.start);

            if (result.start() > tickerWindow.start) {
                assertTrue("Received a final result for the next window: " + result, result.isEarly());
            }

            if (result.isEarly()) {
                assertTrue(windowSize >= result.getValue());
                tickerWindow.hasEarly = true;
            } else {
                if (!tickerWindow.hasEarly) {
                    logger.warning("Not received any early-result for the final-result: " + result);
                }
                assertEquals(windowSize, (long) result.getValue());
                tickerWindow.advance();
            }
        }

        class TickerWindow {

            private long start;
            private boolean hasEarly;

            TickerWindow() {
            }

            void advance() {
                start += windowSize;
                hasEarly = false;
            }
        }
    }
}
