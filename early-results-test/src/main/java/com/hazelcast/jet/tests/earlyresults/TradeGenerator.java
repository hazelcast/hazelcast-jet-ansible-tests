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

package com.hazelcast.jet.tests.earlyresults;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.tests.common.Util.entry;
import static java.util.concurrent.TimeUnit.SECONDS;

final class TradeGenerator {

    private final List<String> tickerList;

    private final long emitPeriod;
    private long timestamp;
    private long lastEmit = System.currentTimeMillis();

    private TradeGenerator(int tradeBatchesPerSec) {
        this.tickerList = loadTickers();
        this.emitPeriod = SECONDS.toMillis(1) / tradeBatchesPerSec;
    }

    private void generateTrades(TimestampedSourceBuffer<Map.Entry<String, Long>> buf) {
        long now = System.currentTimeMillis();
        if (now - lastEmit > emitPeriod) {
            tickerList.stream().map(ticker -> entry(ticker, timestamp)).forEach(trade -> buf.add(trade, timestamp));
            timestamp++;
            lastEmit = now;
        }
    }

    private List<String> loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeGenerator.class.getResourceAsStream("/nasdaqlisted.txt")))) {
            return reader.lines().skip(1).map(l -> l.split("\\|")[0]).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static StreamSource<Map.Entry<String, Long>> tradeSource(int tradesPerSec) {
        return SourceBuilder
                .timestampedStream("trade-source", x -> new TradeGenerator(tradesPerSec))
                .fillBufferFn(TradeGenerator::generateTrades)
                .build();
    }
}
