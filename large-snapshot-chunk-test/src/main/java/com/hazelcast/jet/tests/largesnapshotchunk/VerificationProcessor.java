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

package com.hazelcast.jet.tests.largesnapshotchunk;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.datamodel.KeyedWindowResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

public final class VerificationProcessor extends AbstractProcessor {

    private final Map<String, Long> timePerKey = new HashMap<>();
    private final int windowSize;
    private Traverser<Entry<String, Long>> snapshotTraverser;

    private VerificationProcessor(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        @SuppressWarnings("unchecked")
        KeyedWindowResult<String, List<int[]>> casted = (KeyedWindowResult<String, List<int[]>>) item;
        if (casted.getValue().size() != windowSize) {
            throw new IllegalArgumentException("Expected " + windowSize + " items, but got "
                    + casted.getValue().size());
        }
        Long oldTime = timePerKey.put(casted.getKey(), casted.end());
        if (oldTime == null) {
            oldTime = 0L;
        }
        if (oldTime != casted.end() - windowSize) {
            throw new IllegalArgumentException("Received item for time=" + casted.end() + ", but the last " +
                    "received item for this key was with time=" + oldTime);
        }
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.traverseIterable(timePerKey.entrySet())
                                          .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(Object key, Object value) {
        Long oldValue = timePerKey.put((String) key, (Long) value);
        assert oldValue == null : "Restore called twice for key=" + key;
    }

    static ProcessorMetaSupplier supplier(int windowSize) {
        return preferLocalParallelismOne(ProcessorSupplier.of(() -> new VerificationProcessor(windowSize)));
    }
}
