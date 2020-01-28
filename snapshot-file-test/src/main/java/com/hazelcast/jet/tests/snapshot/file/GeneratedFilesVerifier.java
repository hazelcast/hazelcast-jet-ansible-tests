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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.logging.ILogger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GeneratedFilesVerifier extends Thread {

    private static final String FILE_SINK_DIR_FOR_TEST_PATH = "/tmp/file_sink";

    private static final int SLEEP_AFTER_READ_FILES_MS = 1000;
    private static final int ALLOWED_NO_INPUT_MS = 600000;

    private final Path filesinkDirectory;
    private final Verifier verifier;
    private final String name;
    private final ILogger logger;

    private volatile boolean finished;
    private volatile Throwable error;
    private long lastInputTime;

    public GeneratedFilesVerifier(JetInstance client, String name, ILogger logger) {
        this.name = name;
        this.logger = logger;
        verifier = new Verifier(name, logger);
        filesinkDirectory = Paths.get(FILE_SINK_DIR_FOR_TEST_PATH + name);
    }

    @Override
    public void run() {
        lastInputTime = System.currentTimeMillis();
        while (!finished) {
            try {
                List<Long> processFiles = processFiles();
                long now = System.currentTimeMillis();
                if (processFiles.isEmpty()) {
                    if (now - lastInputTime > ALLOWED_NO_INPUT_MS) {
                        throw new AssertionError(
                                String.format("[%s] No new data was added during last %s", name, ALLOWED_NO_INPUT_MS));
                    }
                } else {
                    lastInputTime = now;
                    verifier.sendToQueue(processFiles);
                }
                Thread.sleep(SLEEP_AFTER_READ_FILES_MS);
            } catch (Exception e) {
                logger.severe("[" + name + "] Exception thrown during processing files.", e);
                error = e;
                finish();
            }
        }
    }

    private List<Long> processFiles() throws IOException {
        try (Stream<Path> list = Files.list(filesinkDirectory)) {
            List<Path> collect = list.collect(Collectors.toList());
            List<String> content = new ArrayList<>();
            for (Path path : collect) {
                List<String> fileContent = Files.readAllLines(path);
                content.addAll(fileContent);
                Files.deleteIfExists(path);
            }
            return content.stream().map(Long::parseLong).collect(Collectors.toList());
        }
    }

    public void finish() {
        verifier.stop();
        finished = true;
    }

    public void checkStatus() {
        if (error != null) {
            throw new RuntimeException(error);
        }
        if (finished) {
            throw new RuntimeException("[" + name + "] GeneratedFilesVerifier is not running");
        }
        verifier.checkStatus();
    }

    public static class Verifier {

        private static final int QUEUE_SIZE_LIMIT = 20_000;
        private static final int PARK_MS = 10;
        private static final int PRINT_LOG = 10_000;

        private final Thread thread;
        private final ConcurrentLinkedQueue<Long> toProcessQueue = new ConcurrentLinkedQueue<>();
        private final ILogger logger;
        private final String name;

        private volatile boolean running = true;
        private volatile Throwable verifierError;

        Verifier(String name, ILogger logger) {
            this.name = name;
            this.logger = logger;
            thread = new Thread(this::run);
            thread.start();
        }

        public void sendToQueue(List<Long> toQueue) {
            for (Long string : toQueue) {
                toProcessQueue.offer(string);
            }
        }

        private void run() {
            try {
                long counter = 0;
                PriorityQueue<Long> verificationQueue = new PriorityQueue<>();
                while (running) {
                    LockSupport.parkNanos(MILLISECONDS.toNanos(PARK_MS));
                    // load new items to verification queue
                    for (Long poll; (poll = toProcessQueue.poll()) != null;) {
                        verificationQueue.offer(poll);
                    }
                    // try to verify head of verification queue
                    for (Long peeked; (peeked = verificationQueue.peek()) != null;) {
                        if (peeked > counter) {
                            // the item might arrive later
                            break;
                        } else if (peeked == counter) {
                            if (counter % PRINT_LOG == 0) {
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
            } catch (Throwable e) {
                logger.severe("[" + name + "] Exception thrown in verifier.", e);
                verifierError = e;
            } finally {
                running = false;
            }
        }

        public void checkStatus() {
            if (verifierError != null) {
                throw new RuntimeException(verifierError);
            }
            if (!running) {
                throw new RuntimeException("[" + name + "] Verifier is not running");
            }
        }

        public void stop() {
            running = false;
            try {
                thread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(verifierError);
            }
            if (verifierError != null) {
                throw new RuntimeException(verifierError);
            }
        }
    }

}
