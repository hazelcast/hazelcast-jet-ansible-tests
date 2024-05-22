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

package com.hazelcast.jet.tests.isolatedjobs;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import java.util.UUID;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public final class Sources {
    private Sources() {
    }

    public static StreamSource<String> createStreamSource(int sleepBetweenStreamRecordsMs) {
        return SourceBuilder.stream("IsolatedJobsStreamSource",
                        context -> context.hazelcastInstance()
                                .getCluster()
                                .getLocalMember()
                                .getSocketAddress()
                                .toString())
                .<String>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx);
                    sleepMillis(sleepBetweenStreamRecordsMs);
                })
                .distributed(1)
                .createSnapshotFn(ctx -> ctx)
                .restoreSnapshotFn((ctx, state) -> ctx = state.get(0))
                .build();
    }

    public static BatchSource<UUID> createBatchSource(long index) {
        return SourceBuilder.batch("IsolatedJobsTestBatchSource" + index, context ->
                        context.hazelcastInstance().getCluster().getLocalMember().getUuid())
                .<UUID>fillBufferFn((uuid, buffer) -> {
                    buffer.add(uuid);
                    buffer.close();
                })
                .distributed(1)
                .build();
    }
}
