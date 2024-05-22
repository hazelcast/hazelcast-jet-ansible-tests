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

package com.hazelcast.jet.tests.s3;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.processor.Processors.noopP;

public final class WordGenerator extends AbstractProcessor {

    private static final int WORDS_PER_LINE = 20;

    private final Traverser traverser;
    private final StringBuilder builder = new StringBuilder();

    private int lineNumber;

    private WordGenerator(int distinctWords, int perMemberWordCount) {
        int lineCount = perMemberWordCount / WORDS_PER_LINE;
        int[] wordNumber = new int[1];
        traverser = () -> {
            if (lineNumber == lineCount) {
                return null;
            }
            for (int i = 0; i < WORDS_PER_LINE; i++) {
                builder.append(wordNumber[0] % distinctWords).append(" ");
                wordNumber[0]++;
            }
            try {
                return builder.toString();
            } finally {
                lineNumber++;
                builder.setLength(0);
            }
        };
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    static ProcessorMetaSupplier metaSupplier(int distinctWords, int totalWordCount) {
        return addresses -> {
            assert totalWordCount % (addresses.size() * WORDS_PER_LINE) == 0;
            int perMemberWordCount = totalWordCount / addresses.size();
            return address -> processorSupplier(distinctWords, perMemberWordCount);
        };
    }

    private static ProcessorSupplier processorSupplier(int distinctWords, int perMemberWordCount) {
        return count -> IntStream
                .range(0, count)
                .mapToObj(i -> i == 0 ? new WordGenerator(distinctWords, perMemberWordCount) : noopP().get())
                .collect(Collectors.toList());
    }

}
