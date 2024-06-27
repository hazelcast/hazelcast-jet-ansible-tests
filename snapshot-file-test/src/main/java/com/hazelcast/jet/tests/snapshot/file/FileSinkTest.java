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

package com.hazelcast.jet.tests.snapshot.file;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;

public class FileSinkTest extends AbstractJetSoakTest {

    private static final String FILE_SINK_LOCATION_ON_MACHINE_PATH = "/tmp/file_sink_directory";

    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 20;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final String DEFAULT_SHUTDOWN_MODE = "TERMINATE";

    private int sleepMsBetweenItem;
    private long snapshotIntervalMs;
    private String jetShutdownMode;

    public static void main(String[] args) throws Exception {
        new FileSinkTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) throws Exception {
        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        jetShutdownMode = property("jetShutdownMode", DEFAULT_SHUTDOWN_MODE);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(HazelcastInstance client, String name) {
        if (jetShutdownMode.equals("TERMINATE") && name.contains(DYNAMIC_CLUSTER)) {
            logger.info("FileSinkTest is ignored for DYNAMIC_CLUSTER in TERMINATE shutdown mode");
            return;
        }

        GeneratedFilesVerifier verifier = new GeneratedFilesVerifier(name, logger);
        verifier.start();

        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(name);
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
            if (name.startsWith(STABLE_CLUSTER)) {
                jobConfig.addClass(FileSinkTest.class, GeneratedFilesVerifier.class);
            }
            Job job = client.getJet().newJob(pipeline(), jobConfig);

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
                .rebalance()
                .writeTo(sink);

        return pipeline;
    }

}
