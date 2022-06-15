package org.apache.openjpa.jdbc.sql.entities;

public class NonSerializableDummy {

    private String name;

    public NonSerializableDummy(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
