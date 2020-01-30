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

import com.hazelcast.logging.ILogger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public class GeneratedFilesVerifier extends Thread {

    private static final String FILE_SINK_DIR_FOR_TEST_PATH = "/tmp/file_sink";

    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_MS = 1000;
    private static final int ALLOWED_NO_INPUT_MS = 600000;
    private static final int QUEUE_SIZE_LIMIT = 20_000;
    private static final int PRINT_LOG_ITEMS = 10_000;

    private final Path filesinkDirectory;
    private final String name;
    private final ILogger logger;

    private volatile boolean finished;
    private volatile Throwable error;
    private long counter;
    private final PriorityQueue<Long> verificationQueue = new PriorityQueue<>();

    public GeneratedFilesVerifier(String name, ILogger logger) {
        this.name = name;
        this.logger = logger;
        filesinkDirectory = Paths.get(FILE_SINK_DIR_FOR_TEST_PATH + name);
    }

    @Override
    public void run() {
        long lastInputTime = System.currentTimeMillis();
        while (!finished) {
            try {
                List<Long> processFiles = processFiles();
                for (Long processFile : processFiles) {
                    verificationQueue.add(processFile);
                }
                long now = System.currentTimeMillis();
                if (processFiles.isEmpty()) {
                    if (now - lastInputTime > ALLOWED_NO_INPUT_MS) {
                        throw new AssertionError(
                                String.format("[%s] No new data was added during last %s", name, ALLOWED_NO_INPUT_MS));
                    }
                } else {
                    verifyQueue();
                    lastInputTime = now;
                }
                Thread.sleep(SLEEP_AFTER_VERIFICATION_CYCLE_MS);
            } catch (Throwable e) {
                logger.severe("[" + name + "] Exception thrown during processing files.", e);
                error = e;
                finished = true;
            }
        }
    }

    private List<Long> processFiles() throws IOException {
        try (Stream<Path> fileList = Files.list(filesinkDirectory)) {
            return fileList
                    .map(path -> uncheckCall(() -> {
                        List<String> lines = Files.readAllLines(path);
                        Files.deleteIfExists(path);
                        return lines;
                    }))
                    .flatMap(Collection::stream)
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
        }
    }

    private void verifyQueue() {
        // try to verify head of verification queue
        for (Long peeked; (peeked = verificationQueue.peek()) != null;) {
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

    public void finish() {
        finished = true;
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
