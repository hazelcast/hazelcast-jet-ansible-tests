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
