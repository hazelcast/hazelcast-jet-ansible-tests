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

package com.hazelcast.jet.tests.kafkaconnectsource;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Order implements Serializable {
    final Integer orderId;
    final long orderTime;
    final String itemId;
    final double orderUnits;
    final Map<String, String> headers = new HashMap<>();

    Order(SourceRecord rec) {
        Struct struct = Values.convertToStruct(rec.valueSchema(), rec.value());
        orderId = struct.getInt32("orderid");
        orderTime = struct.getInt64("ordertime");
        itemId = struct.getString("itemid");
        orderUnits = struct.getFloat64("orderunits");
        for (Header header : rec.headers()) {
            headers.put(header.key(), header.value().toString());
        }
    }

    @Override
    public String toString() {
        return "Order{" +
               "orderId=" + orderId +
               ", orderTime=" + orderTime +
               ", itemtId='" + itemId + '\'' +
               ", orderUnits=" + orderUnits +
               ", headers=" + headers +
               '}';
    }
}
