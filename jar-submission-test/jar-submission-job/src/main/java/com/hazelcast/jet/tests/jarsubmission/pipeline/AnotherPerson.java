package com.hazelcast.jet.tests.jarsubmission.pipeline;

import java.io.Serializable;

public class AnotherPerson implements Serializable {

    private String name;

    public AnotherPerson(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
