package com.latticeengines.telepath.testartifacts.classes;

public class D extends TestClassBase {

    private String foreignKeyB;
    public D(String id, String namespace, String foreignKeyB) {
        setId(id);
        setNamespace(namespace);
        setForeignKeyB(foreignKeyB);
    }

    public String getForeignKeyB() {
        return foreignKeyB;
    }

    public void setForeignKeyB(String foreignKeyB) {
        this.foreignKeyB = foreignKeyB;
    }
}
