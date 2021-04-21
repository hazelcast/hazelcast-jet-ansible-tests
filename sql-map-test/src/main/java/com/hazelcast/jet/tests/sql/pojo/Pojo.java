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
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class Pojo implements Serializable {

    private boolean booleanVal;

    private byte tinyIntVal;
    private short smallIntVal;
    private int intVal;
    private long bigIntVal;
    private float realVal;
    private double doubleVal;

    private BigInteger decimalBigIntegerVal;
    private BigDecimal decimalVal;

    private char charVal;
    private String varcharVal;

    private LocalTime timeVal;
    private LocalDate dateVal;
    private LocalDateTime timestampVal;

    private Date tsTzDateVal;
    private GregorianCalendar tsTzCalendarVal;
    private Instant tsTzInstantVal;
    private OffsetDateTime tsTzOffsetDateTimeVal;
    private ZonedDateTime tsTzZonedDateTimeVal;

    private List<Object> objectVal;

    public Pojo(long val) {
        booleanVal = val % 2 == 0;

        tinyIntVal = (byte) val;
        smallIntVal = (short) val;
        intVal = (int) val;
        bigIntVal = val;
        realVal = (float) val;
        doubleVal = (double) val;

        decimalBigIntegerVal = BigInteger.valueOf(val);
        decimalVal = BigDecimal.valueOf(val);

        charVal = 'c';
        varcharVal = Long.toString(val);

        timestampVal = LocalDateTime.now();
        dateVal = timestampVal.toLocalDate();
        timeVal = timestampVal.toLocalTime();

        tsTzDateVal = new Date();
        tsTzCalendarVal = (GregorianCalendar) GregorianCalendar.getInstance();
        tsTzInstantVal = Instant.now();
        tsTzOffsetDateTimeVal = OffsetDateTime.now();
        tsTzZonedDateTimeVal = ZonedDateTime.now();

        objectVal = new ArrayList<>(1);
        objectVal.add(val);
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

    public BigInteger getDecimalBigIntegerVal() {
        return decimalBigIntegerVal;
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

    public LocalTime getTimeVal() {
        return timeVal;
    }

    public LocalDate getDateVal() {
        return dateVal;
    }

    public LocalDateTime getTimestampVal() {
        return timestampVal;
    }

    public Date getTsTzDateVal() {
        return tsTzDateVal;
    }

    public GregorianCalendar getTsTzCalendarVal() {
        return tsTzCalendarVal;
    }

    public Instant getTsTzInstantVal() {
        return tsTzInstantVal;
    }

    public OffsetDateTime getTsTzOffsetDateTimeVal() {
        return tsTzOffsetDateTimeVal;
    }

    public ZonedDateTime getTsTzZonedDateTimeVal() {
        return tsTzZonedDateTimeVal;
    }

    public List<Object> getObjectVal() {
        return objectVal;
    }
}
