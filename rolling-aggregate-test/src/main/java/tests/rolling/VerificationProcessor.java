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

package tests.rolling;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

public class VerificationProcessor extends AbstractProcessor {

    private long max;
    private IAtomicLong persist;
    private boolean processed;

    public static ProcessorMetaSupplier supplier() {
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
    protected void init(Context context) {
        persist = context.jetInstance().getHazelcastInstance().getAtomicLong("persist");
        max = persist.get();
    }

    @Override
    public boolean saveToSnapshot() {
        if (processed) {
            persist.set(max);
        }
        return true;
    }
}
