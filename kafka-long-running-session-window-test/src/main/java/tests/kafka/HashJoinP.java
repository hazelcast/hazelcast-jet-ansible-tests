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
import com.hazelcast.jet.datamodel.Session;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashJoinP extends AbstractProcessor {

    private Map<String, Double> map = new ConcurrentHashMap<>();

    public HashJoinP() {
    }

    @Override
    protected boolean tryProcess0(Object item) throws Exception {
        // Records read from Kafka
        Trade trade = (Trade) item;
        map.put(trade.getTicker(), trade.getPrice());
        return true;
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) throws Exception {
        // items come to ordinals >= 1
        Session<String, Double> session = (Session<String, Double>) item;

        String ticker = session.getKey();
        Double sum = map.get(ticker);

        if (sum == null) {
            return false;
        }
        if (sum.equals(round(session.getResult(), 2))) {
            return tryEmit(new SimpleImmutableEntry<>(ticker, sum));
        } else {
            throw new AssertionError("produced results are not matching for ticker -> "
                    + ticker + " expected -> " + sum + ", actual -> " + session.getResult());
        }
    }

    private static double round(double value, int places) {
        if (places < 0) {
            throw new IllegalArgumentException();
        }

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }


    public static ProcessorSupplier getSupplier() {
        return count -> IntStream.range(0, count).mapToObj(operand -> new HashJoinP()).collect(Collectors.toList());

    }
}
