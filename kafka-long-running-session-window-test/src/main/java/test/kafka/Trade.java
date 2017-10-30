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

package test.kafka;

import java.io.Serializable;

public class Trade implements Serializable{

    private final long time;
    private final String ticker;
    private final int quantity;
    private final double price; // in cents

    Trade(long time, String ticker, int quantity, double price) {
        this.time = time;
        this.ticker = ticker;
        this.quantity = quantity;
        this.price = price;
    }

    public long getTime() {
        return time;
    }

    public String getTicker() {
        return ticker;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Trade{time=" + time + ", ticker='" + ticker + '\'' + ", quantity=" + quantity + ", price=" + price + '}';
    }
}
