package com.hazelcast.jet.sql.tests.tumblewindow;

import com.hazelcast.sql.SqlRow;

import java.util.stream.IntStream;

public class MaxTumbleWindowTest extends AbstractTumbleWindowTest {

    private static final String SOURCE = "MAX_TRADES_SOURCE";
    private static final String SINK = "MAX_TRADES_SINK";
    private static final String AGGREGATION_TYPE = "MAX";

    public MaxTumbleWindowTest(String sourceName, String sinkName, String aggregationType) {
        super(sourceName, sinkName, aggregationType);
    }

    public static void main(String[] args) throws Exception {
        new MaxTumbleWindowTest(SOURCE, SINK, AGGREGATION_TYPE).run(args);
    }

    @Override
    protected void assertQuerySuccessful(SqlRow sqlRow, int currentEventStartTime, int currentEventEndTime) {
        int actualAvgValue = sqlRow.getObject(0);
        int expectedValue = IntStream.range(currentEventStartTime, currentEventEndTime).max().getAsInt();
        assertEquals("The max count over aggregate window does not match", expectedValue, actualAvgValue);
    }

}
