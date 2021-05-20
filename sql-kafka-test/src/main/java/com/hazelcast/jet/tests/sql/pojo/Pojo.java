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

package com.hazelcast.jet.tests.sql.pojo;

import java.io.Serializable;
import java.math.BigDecimal;

public class Pojo implements Serializable {

    private boolean booleanVal;

    private byte tinyIntVal;
    private short smallIntVal;
    private int intVal;
    private long bigIntVal;
    private float realVal;
    private double doubleVal;
    private long timestampVal;

    private BigDecimal decimalVal;

    private char charVal;
    private String varcharVal;

    public Pojo() { }    //This is needed for Jackson deserialization

    public Pojo(long val) {
        booleanVal = val % 2 == 0;

        tinyIntVal = (byte) val;
        smallIntVal = (short) val;
        intVal = (int) val;
        bigIntVal = val;
        realVal = (float) val;
        doubleVal = (double) val;
        timestampVal = System.currentTimeMillis();

        decimalVal = BigDecimal.valueOf(val);

        charVal = 'c';
        varcharVal = Long.toString(val);

    }

    public boolean isBooleanVal() {
        return booleanVal;
    }

    public byte getTinyIntVal() {
        return tinyIntVal;
    }

    public short getSmallIntVal() {
        return smallIntVal;
    }

    public int getIntVal() {
        return intVal;
    }

    public long getBigIntVal() {
        return bigIntVal;
    }

    public float getRealVal() {
        return realVal;
    }

    public double getDoubleVal() {
        return doubleVal;
    }

    public long getTimestampVal() {
        return timestampVal;
    }

    public BigDecimal getDecimalVal() {
        return decimalVal;
    }

    public char getCharVal() {
        return charVal;
    }

    public String getVarcharVal() {
        return varcharVal;
    }

}
