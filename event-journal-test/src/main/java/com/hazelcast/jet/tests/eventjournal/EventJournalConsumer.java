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

package com.hazelcast.jet.tests.eventjournal;

import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.ReadResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;


public class EventJournalConsumer<K, V> {

    private static final int POLL_COUNT = 20;

    private final ClientMapProxy<K, V> proxy;
    private final int partitionCount;
    private final long[] offsets;
    private final PredicateEx<EventJournalMapEvent<K, V>> predicate;

    public EventJournalConsumer(IMap<K, V> map, PredicateEx<EventJournalMapEvent<K, V>> predicate,
                                int partitionCount) {
        this.proxy = (ClientMapProxy<K, V>) map;
        this.predicate = predicate;
        this.partitionCount = partitionCount;
        offsets = new long[partitionCount];
    }

    public boolean drain(Consumer<EventJournalMapEvent<K, V>> consumer) throws Exception {
        boolean isEmpty = true;
        List<CompletableFuture<ReadResultSet<EventJournalMapEvent<K, V>>>> futureList = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            CompletableFuture<ReadResultSet<EventJournalMapEvent<K, V>>> f = proxy.readFromEventJournal(
                    offsets[i], 0, POLL_COUNT, i, null, null);
            futureList.add(f);
        }
        for (int i = 0; i < partitionCount; i++) {
            CompletableFuture<ReadResultSet<EventJournalMapEvent<K, V>>> future = futureList.get(i);
            ReadResultSet<EventJournalMapEvent<K, V>> resultSet = future.get();
            StreamSupport.stream(resultSet.spliterator(), false).filter(predicate).forEach(consumer);
            offsets[i] = offsets[i] + resultSet.readCount();
            isEmpty = isEmpty & resultSet.readCount() == 0;
        }
        return isEmpty;
    }


}
