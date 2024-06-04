package com.hazelcast.jet.tests.common;

public enum ClusterType {
    STABLE("Stable"), DYNAMIC("Dynamic");

    private final String prettyName;

    ClusterType(String prettyName) {

        this.prettyName = prettyName;
    }

    public String getPrettyName() {
        return prettyName;
    }
}
