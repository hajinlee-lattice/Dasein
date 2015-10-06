package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public interface AttributeOwner extends HasPid, HasName, GraphNode {
    
    void addAttribute(String attribute);

    List<String> getAttributes();

    void setAttributes(List<String> attributes);

    String[] getAttributeNames();
}
