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

package com.hazelcast.jet.tests.stateful;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicate;

import java.util.Map;

public class VerificationEntryProcessor implements EntryProcessor<Long, Long, Integer> {
    @Override
    public Integer process(Map.Entry<Long, Long> entry) {
        Long value = entry.getValue();
        entry.setValue(null);
        return value == StatefulMapTest.TIMED_OUT_CODE ? 0 : 1;
    }

    static Predicate<Long, Long> predicate(long maxKey) {
        return mapEntry -> mapEntry.getKey() < maxKey;
    }
}
