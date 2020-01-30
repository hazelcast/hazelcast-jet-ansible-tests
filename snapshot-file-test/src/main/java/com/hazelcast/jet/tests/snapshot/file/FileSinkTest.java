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

package com.hazelcast.jet.tests.snapshot.file;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileSinkTest extends AbstractSoakTest {

    private static final String STABLE = "Stable";
    private static final String DYNAMIC = "Dynamic";

    private static final String FILE_SINK_LOCATION_ON_MACHINE_PATH = "/tmp/file_sink_directory";

    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 20;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;

    private int sleepMsBetweenItem;
    private long snapshotIntervalMs;

    private JetInstance stableClusterClient;

    public static void main(String[] args) throws Exception {
        new FileSinkTest().run(args);
    }

    @Override
    public void init() throws Exception {
        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);

        stableClusterClient = Jet.newJetClient(remoteClusterClientConfig());
    }

    @Override
    public void test() throws Throwable {
        Throwable[] exceptions = new Throwable[2];
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                testInternal(jet, DYNAMIC);
            } catch (Throwable t) {
                logger.severe("Exception in Dynamic cluster test", t);
                exceptions[0] = t;
            }
        });
        executorService.execute(() -> {
            try {
                testInternal(stableClusterClient, STABLE);
            } catch (Throwable t) {
                logger.severe("Exception in Stable cluster test", t);
                exceptions[1] = t;
            }
        });
        executorService.shutdown();
        executorService.awaitTermination(durationInMillis, MILLISECONDS);

        if (exceptions[0] != null) {
            logger.severe("Exception in Dynamic cluster test", exceptions[0]);
        }
        if (exceptions[1] != null) {
            logger.severe("Exception in Stable cluster test", exceptions[1]);
        }
        if (exceptions[0] != null) {
            throw exceptions[0];
        }
        if (exceptions[1] != null) {
            throw exceptions[1];
        }
    }

    public void testInternal(JetInstance client, String name) {
        GeneratedFilesVerifier verifier = new GeneratedFilesVerifier(name, logger);
        verifier.start();

        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(String.format("FileSinkTest(%s)", name));
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
            if (name.equals(STABLE)) {
                jobConfig.addClass(FileSinkTest.class, GeneratedFilesVerifier.class);
            }
            Job job = client.newJob(pipeline(), jobConfig);

            try {
                long begin = System.currentTimeMillis();
                while (System.currentTimeMillis() - begin < durationInMillis) {
                    if (getJobStatusWithRetry(job) == FAILED) {
                        job.join();
                    }
                    verifier.checkStatus();
                    sleepMinutes(1);
                }
            } finally {
                job.cancel();
            }
        } finally {
            verifier.finish();
        }
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        if (stableClusterClient != null) {
            stableClusterClient.shutdown();
        }
    }

    private Pipeline pipeline() {
        int sleep = sleepMsBetweenItem;

        Pipeline pipeline = Pipeline.create();

        StreamSource<Long> source = SourceBuilder
                .stream("srcForFileSink", procCtx -> new long[1])
                .<Long>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx[0]++);
                    sleepMillis(sleep);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();

        Sink<Object> sink = Sinks.filesBuilder(FILE_SINK_LOCATION_ON_MACHINE_PATH)
                .exactlyOnce(true)
                .build();

        pipeline.readFrom(source)
                .withoutTimestamps()
                .groupingKey(identity())
                .filterUsingService(sharedService(ctx -> null), (s, k, v) -> true)
                .writeTo(sink);

        return pipeline;
    }

}
