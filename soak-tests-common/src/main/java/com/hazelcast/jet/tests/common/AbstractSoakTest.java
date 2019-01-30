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

package com.hazelcast.jet.tests.common;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.server.JetBootstrap;
import org.junit.Before;

import java.io.Serializable;

import static java.util.concurrent.TimeUnit.MINUTES;

public class AbstractSoakTest implements Serializable {

    private static final int DEFAULT_DURATION_MINUTES = 30;

    protected transient JetInstance jet;

    @Before
    public void setupBase() {
        jet = JetBootstrap.getInstance();
    }

    protected String property(String name, String defaultValue) {
        return System.getProperty(name, defaultValue);
    }

    protected long durationInMillis() {
        return MINUTES.toMillis(propertyInt("durationInMinutes", DEFAULT_DURATION_MINUTES));
    }

    protected int propertyInt(String name, int defaultValue) {
        String value = System.getProperty(name);
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }
}
