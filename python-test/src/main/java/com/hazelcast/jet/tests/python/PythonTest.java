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

package com.hazelcast.jet.tests.python;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.python.PythonServiceConfig;
import com.hazelcast.jet.python.PythonTransforms;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.map.IMap;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.query.Predicates.alwaysTrue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class PythonTest extends AbstractJetSoakTest {

    private static final String SOURCE_MAP_NAME = "pythonSourceMap";
    private static final String SINK_LIST_NAME = "pythonSinkList";
    private static final String PYTHON_BASE_DIR = "python_base_dir";
    private static final String PYTHON_HANDLER_MODULE = "echo";
    private static final String PYTHON_HANDLER_FUNCTION = "handle";
    private static final String ECHO_HANDLER_FUNCTION =
            "def handle(input_list):\n" +
                    "    return ['echo-%s' % i for i in input_list]\n";

    private static final int DEFAULT_ITEM_COUNT = 10_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int DEFAULT_SLEEP_AFTER_TEST_SECONDS = 5;

    private File baseDir;
    private IMap<Integer, String> sourceMap;
    private IList<String> sinkList;
    private int itemCount;
    private int sleepAfterTestSeconds;

    public static void main(String[] args) throws Exception {
        new PythonTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        baseDir = createTempDirectory();

        sourceMap = client.getMap(SOURCE_MAP_NAME);
        sinkList = client.getList(SINK_LIST_NAME);

        sourceMap.clear();
        sinkList.clear();

        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        sleepAfterTestSeconds = propertyInt("sleepAfterTestSeconds", DEFAULT_SLEEP_AFTER_TEST_SECONDS);

        installFileToBaseDir();
        range(0, itemCount)
                .mapToObj(i -> sourceMap.putAsync(i, String.valueOf(i)).toCompletableFuture())
                .collect(toList())
                .forEach(f -> uncheckCall(f::get));
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("PythonTest" + jobCount);
            client.getJet().newJob(pipeline(), jobConfig).join();

            assertEquals(itemCount, sinkList.size());
            assertEquals(itemCount, sinkList.stream().filter(s -> s.startsWith("echo-")).count());
            sinkList.clear();
            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCount);
            }
            jobCount++;
            sleepSeconds(sleepAfterTestSeconds);
        }
        logger.info("Final job count: " + jobCount);
    }

    @Override
    protected void teardown(Throwable t) {
        IOUtil.delete(baseDir);
    }

    private Pipeline pipeline() {
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule(PYTHON_HANDLER_MODULE)
                .setHandlerFunction(PYTHON_HANDLER_FUNCTION);
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(sourceMap, alwaysTrue(), Map.Entry::getValue))
         .apply(PythonTransforms.mapUsingPythonBatch(cfg))
         .writeTo(Sinks.list(sinkList));

        return p;
    }

    private void installFileToBaseDir() throws IOException {
        try (InputStream in = new ByteArrayInputStream(ECHO_HANDLER_FUNCTION.getBytes(UTF_8))) {
            Files.copy(in, new File(baseDir, PYTHON_HANDLER_MODULE + ".py").toPath());
        }
    }

    private static File createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory(PYTHON_BASE_DIR);
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }
}
