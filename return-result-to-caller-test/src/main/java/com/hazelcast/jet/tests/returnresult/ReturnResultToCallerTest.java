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

package com.hazelcast.jet.tests.returnresult;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.BasicEventJournalProducer;
import com.hazelcast.logging.ILogger;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static com.hazelcast.jet.tests.common.Util.entry;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class ReturnResultToCallerTest extends AbstractJetSoakTest {

    private static final String SOURCE = ReturnResultToCallerTest.class.getSimpleName();
    private static final int EVENT_JOURNAL_CAPACITY = 600_000;

    private static final int DEFAULT_WINDOW_SIZE = 200;
    private static final int DEFAULT_ALLOWED_LAG = 2000;
    private static final int DEFAULT_LOG_PROGRESS = 10000;

    private static final String OBSERVABLE_NAME = ReturnResultToCallerTest.class.getSimpleName();

    private static int windowSize;
    private static int allowedLag;
    private static int logProgress;

    private transient BasicEventJournalProducer producer;

    private Observable<Map.Entry<Long, Long>> observable;
    private TestObserver observer;

    public static void main(String[] args) throws Exception {
        new ReturnResultToCallerTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) {
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        allowedLag = propertyInt("allowedLag", DEFAULT_ALLOWED_LAG);
        logProgress = propertyInt("logProgress", DEFAULT_LOG_PROGRESS);

        observable = client.getJet().getObservable(OBSERVABLE_NAME);
        observer = new TestObserver(logger);
        observable.addObserver(observer);

        producer = new BasicEventJournalProducer(client, SOURCE, EVENT_JOURNAL_CAPACITY);
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Throwable {
        JobConfig jobConfig = new JobConfig()
                .setName(name);
        Job job = client.getJet().newJob(pipeline(), jobConfig);
        waitForJobStatus(job, RUNNING);
        producer.start();
        sleepMinutes(1);

        int lastTs = -1;

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobStatus jobStatus = getJobStatusWithRetry(job);
            assertEquals(RUNNING, jobStatus);

            Throwable onNextError = observer.getOnNextError();
            if (onNextError != null) {
                throw onNextError;
            }

            Throwable error = observer.getError();
            if (error != null) {
                throw error;
            }
            assertEquals(0, observer.getCompletions());

            int newLastTs = observer.getLastTs();
            assertTrue("Observer did not see any new item. Last viewed item: " + lastTs, newLastTs > lastTs);

            lastTs = newLastTs;

            sleepMinutes(1);
        }

        job.cancel();
    }

    private Pipeline pipeline() {

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Long, Long, Long>mapJournal(SOURCE,
                START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                .withTimestamps(t -> t, allowedLag)
                .window(tumbling(windowSize))
                .aggregate(counting())
                .map(t -> entry(t.start(), t.result()))
                .writeTo(Sinks.observable(OBSERVABLE_NAME));
        return p;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        if (observable != null) {
            observable.destroy();
        }
        if (producer != null) {
            producer.stop();
        }
    }

    private static final class TestObserver implements Observer<Map.Entry<Long, Long>> {

        private final ILogger logger;
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicReference<Throwable> onNextError = new AtomicReference<>();
        private int completions;
        private int lastTs = -windowSize;

        TestObserver(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void onNext(Map.Entry<Long, Long> entry) {
            if (entry.getKey() % logProgress == 0) {
                logger.info("Current progress: <" + entry.getKey() + ", " + entry.getValue() + ">");
            }
            int expectedTs = lastTs + windowSize;
            try {
                assertEquals(expectedTs, (long) entry.getKey());
                assertEquals("Incorrect item count for window started with " + entry.getKey() + ", expected: "
                        + windowSize + ", but was: " + entry.getValue(), windowSize, (long) entry.getValue());
            } catch (Throwable ex) {
                onNextError.set(ex);
                throw ex;
            }
            lastTs = expectedTs;
        }

        @Override
        public void onError(Throwable throwable) {
            logger.info("New onError: <" + throwable.getMessage() + ">");
            error.set(throwable);
        }

        @Override
        public void onComplete() {
            logger.info("New onComplete.");
            completions++;
        }

        Throwable getError() {
            return error.get();
        }

        Throwable getOnNextError() {
            return onNextError.get();
        }

        public int getCompletions() {
            return completions;
        }

        public int getLastTs() {
            return lastTs;
        }

    }

}
