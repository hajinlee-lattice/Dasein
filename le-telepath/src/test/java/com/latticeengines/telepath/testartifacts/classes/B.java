package com.latticeengines.telepath.testartifacts.classes;

public class B extends TestClassBase {
    private String foreignKeyC;

    public B(String id, String namespace, String foreignKeyC) {
        setId(id);
        setNamespace(namespace);
        setForeignKeyC(foreignKeyC);
    }

    public String getForeignKeyC() {
        return foreignKeyC;
    }

    public void setForeignKeyC(String foreignKeyC) {
        this.foreignKeyC = foreignKeyC;
    }
}
