package com.latticeengines.domain.exposed.eai;

import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Table extends AttributeOwner implements HasName {

    private String name;
    private String displayName;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }
}
