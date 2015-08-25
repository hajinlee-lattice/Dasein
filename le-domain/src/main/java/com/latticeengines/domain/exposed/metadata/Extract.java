package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Extract implements HasName, GraphNode {

    private String name;
    private String path;
    private Long extractionTimestamp;
    
    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }

    public Long getExtractionTimestamp() {
        return extractionTimestamp;
    }

    public void setExtractionTimestamp(Long extractionTimestamp) {
        this.extractionTimestamp = extractionTimestamp;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        return new ArrayList<>();
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

}
