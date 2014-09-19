package com.latticeengines.domain.exposed.eai;

import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Attribute implements HasName {

    private String name;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
