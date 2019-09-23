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

import java.io.Serializable;

/**
 * todo add proper javadoc
 */
public class TransactionEvent implements Serializable {

    private final Type type;
    private final long transactionId;
    private final long timestamp;

    public enum Type {
        START, END
    }

    public TransactionEvent(Type type, long transactionId, long timestamp) {
        this.type = type;
        this.transactionId = transactionId;
        this.timestamp = timestamp;
    }

    public Type type() {
        return type;
    }

    public long transactionId() {
        return transactionId;
    }

    public long timestamp() {
        return timestamp;
    }
}
