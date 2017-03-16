package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Sort implements GraphNode {
    @JsonProperty("lookups")
    private List<Lookup> lookups;
    @JsonProperty("descending")
    private boolean descending;

    public Sort(List<Lookup> lookups, boolean descending) {
        this.lookups = lookups;
        this.descending = descending;
    }

    public Sort(List<Lookup> lookups) {
        this.lookups = lookups;
    }

    public Sort() {
        this(new ArrayList<>());
    }

    public List<Lookup> getLookups() {
        return lookups;
    }

    public void setLookups(List<Lookup> lookups) {
        this.lookups = lookups;
    }

    public boolean getDescending() {
        return descending;
    }

    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public void addLookup(Lookup lookup) {
        this.lookups.add(lookup);
    }

    @SuppressWarnings("unchecked")
    public void setLookups(SchemaInterpretation objectType, String... columnNames) {
        this.lookups = new ArrayList<String>(Arrays.asList(columnNames)).stream() //
                .map((columnName) -> new ColumnLookup(objectType, columnName)) //
                .collect(Collectors.toList());
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        return lookups.stream().collect(Collectors.toList());
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("lookups", lookups);
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }
}
