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

import com.hazelcast.core.IMap;

public class EventJournalTradeProducer implements AutoCloseable {

    private static final int SLEEPY_MILLIS = 10;

    private volatile boolean running = true;

    public EventJournalTradeProducer() {
    }

    @Override
    public void close() {
        running = false;
    }


    public void produce(IMap<Long, Long> map, int countPerTicker) {
        for (long i = 0; running; i++) {
            try {
                Thread.sleep(SLEEPY_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int j = 0; j < countPerTicker; j++) {
                map.set(i, i);
            }
        }
    }

}
