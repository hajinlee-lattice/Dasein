package com.latticeengines.domain.exposed.query;

public class SelectAllLookup extends Lookup {
    private String alias;

    public SelectAllLookup(String alias) {
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }
}
