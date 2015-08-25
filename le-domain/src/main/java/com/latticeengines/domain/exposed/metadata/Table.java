package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Table extends AttributeOwner implements HasName {

    private String name;
    private String displayName;
    private List<Extract> extracts = new ArrayList<>();
    private PrimaryKey primaryKey;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("display_name")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("display_name")
    public String getDisplayName() {
        return displayName;
    }

    public List<Extract> getExtracts() {
        return extracts;
    }

    public void addExtract(Extract extract) {
        extracts.add(extract);
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    @JsonIgnore
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.addAll(super.getChildren());
        children.add(primaryKey);
        children.addAll(extracts);
        return children;
    }

    @Override
    @JsonIgnore
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = super.getChildMap();
        map.put("primaryKey", Arrays.<GraphNode>asList(new GraphNode[] { primaryKey }));
        map.put("extracts", extracts);
        return map;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
