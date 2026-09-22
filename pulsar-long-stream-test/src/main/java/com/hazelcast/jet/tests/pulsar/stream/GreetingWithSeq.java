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

package com.hazelcast.jet.tests.pulsar.stream;

import java.io.Serial;
import java.io.Serializable;

/**
 * Message published to the input topic by {@link PulsarMessageProducer}. Carries a
 * {@code favouriteMeal} field (used to build the grouped/keyed {@code mapStateful} stage, the
 * "pipeline complexity" ported from {@code pulsar-test}'s {@code Greeting}) alongside a globally
 * monotonic {@code seq}, which is threaded untouched through both pipeline hops so that
 * {@link com.hazelcast.jet.tests.common.VerificationProcessor} keeps verifying the same single
 * global sequence it always has.
 */
public record GreetingWithSeq(String name, String favouriteMeal, long seq) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
}
