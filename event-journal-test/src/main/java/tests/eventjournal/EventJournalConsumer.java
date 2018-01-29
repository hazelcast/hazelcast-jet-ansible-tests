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

package tests.eventjournal;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.ringbuffer.ReadResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * todo add proper javadoc
 */
public class EventJournalConsumer<K, V> {

    private static final int POLL_COUNT = 10;

    private final ClientMapProxy<K, V> proxy;

    private final int partitionCount;

    private final long[] offsets;

    public EventJournalConsumer(ClientMapProxy<K, V> proxy, int partitionCount) {
        this.proxy = proxy;
        this.partitionCount = partitionCount;
        offsets = new long[partitionCount];
    }

    public List<EventJournalMapEvent<K, V>> poll() throws ExecutionException, InterruptedException {
        List<EventJournalMapEvent<K, V>> resultList = new ArrayList<>();
        List<ICompletableFuture<ReadResultSet<Object>>> futureList = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            ICompletableFuture<ReadResultSet<Object>> f = proxy.readFromEventJournal(
                    offsets[i], 0, POLL_COUNT, i, null, null);
            futureList.add(f);
        }
        for (int i = 0; i < partitionCount; i++) {
            ICompletableFuture<ReadResultSet<Object>> future = futureList.get(i);
            ReadResultSet<Object> resultSet = future.get();
            for (Object o : resultSet) {
                resultList.add((EventJournalMapEvent<K, V>) o);
            }
            offsets[i] = offsets[i] + resultSet.readCount();
        }
        return resultList;
    }

}
