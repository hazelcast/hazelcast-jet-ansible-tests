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
 * Message published to the middle topic: {@link GreetingWithSeq} enriched with the per-meal
 * running count ({@code howMany}) computed by the first hop's {@code mapStateful} stage. This is
 * purely additional/informational state - {@code seq} is carried through untouched from
 * {@link GreetingWithSeq}, so the second hop can project it back out and feed the same global,
 * monotonic sequence into {@link com.hazelcast.jet.tests.common.VerificationProcessor} that the
 * single-hop version of this test always has.
 */
public record GreetingWithStatAndSeq(String name, String favouriteMeal, int howMany, long seq)
        implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
}
