package com.latticeengines.domain.exposed.metadata;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class PrimaryKey extends AttributeOwner implements HasName, GraphNode {
    private String name;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }
    

}
