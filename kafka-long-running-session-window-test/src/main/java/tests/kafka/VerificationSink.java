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

import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

public class VerificationSink extends AbstractProcessor {

    private static final int VERIFIED_COUNT_TO_LOG = 10;

    private long expectedCount;
    private long verifiedCount;

    public VerificationSink(long expectedCount) {
        this.expectedCount = expectedCount;
    }

    @Override
    protected boolean tryProcess0(Object item) throws Exception {
        Entry<String, Long> entry = (Entry<String, Long>) item;
        String ticker = entry.getKey();
        long count = entry.getValue();

        assertEquals("Unexpected count for " + ticker, expectedCount, count);

        if (++verifiedCount % VERIFIED_COUNT_TO_LOG == 0) {
            getLogger().info("Verified window count for long running kafka test: " + verifiedCount);
        }
        return true;
    }
}
