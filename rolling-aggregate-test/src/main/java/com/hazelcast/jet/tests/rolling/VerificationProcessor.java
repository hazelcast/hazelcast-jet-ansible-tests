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

package com.hazelcast.jet.tests.rolling;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

public class VerificationProcessor extends AbstractProcessor {

    private long max = -1;
    private boolean processed;

    static ProcessorMetaSupplier supplier() {
        return preferLocalParallelismOne(ProcessorSupplier.of(VerificationProcessor::new));
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        processed = true;
        long newMax = (Long) item;
        if (max > newMax) {
            throw new IllegalArgumentException("Trying to set a smaller value. old:" + max + ", new: " + newMax);
        }
        max = newMax;
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!processed) {
            return true;
        }
        return tryEmitToSnapshot(BroadcastKey.broadcastKey(max), max);
    }

    @Override
    protected void restoreFromSnapshot(Object key, Object value) {
        assert max == -1 : "Restore called twice, currentValue: " + max + ", newValue: " + value;
        max = (long) value;
    }
}
