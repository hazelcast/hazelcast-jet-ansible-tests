package com.hazelcast.jet.tests.jarsubmission.person;

import java.io.Serializable;

public class Person implements Serializable {

    private String name;

    public Person(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
