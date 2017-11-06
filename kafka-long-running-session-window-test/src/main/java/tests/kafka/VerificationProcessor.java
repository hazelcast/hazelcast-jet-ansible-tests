/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package tests.kafka;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VerificationProcessor extends AbstractProcessor {


    private int expectedCount;

    public VerificationProcessor(int expectedCount) {
        this.expectedCount = expectedCount;
    }

    @Override
    protected boolean tryProcess0(Object item) throws Exception {
        Entry<String, Long> entry = (Entry<String, Long>) item;
        String ticker = entry.getKey();
        Long count = entry.getValue();

        if (count.intValue() == expectedCount) {
            return tryEmit(new SimpleImmutableEntry<>(ticker, count));
        } else {
            throw new AssertionError("produced results are not matching for ticker -> "
                    + ticker + " expected -> " + expectedCount + ", actual -> " + count);
        }
    }


    public static ProcessorSupplier getSupplier(int expectedCount) {
        return count -> IntStream.range(0, count).mapToObj(operand ->
                new VerificationProcessor(expectedCount)).collect(Collectors.toList());

    }
}
