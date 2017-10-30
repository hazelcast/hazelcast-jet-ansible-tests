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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class TradeSerializer implements Serializer<Trade> {
    private final ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Trade trade) {
        DataOutputStream out = new DataOutputStream(outStream);
        try {
            out.writeUTF(trade.getTicker());
            out.writeLong(trade.getTime());
            out.writeInt(trade.getPrice());
            out.writeInt(trade.getQuantity());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] bytes = outStream.toByteArray();
        outStream.reset();
        return bytes;
    }

    @Override
    public void close() {
        try {
            outStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
