package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.latticeengines.common.exposed.graph.GraphNode;

public class AttrDimension implements GraphNode, Serializable {

    private static final long serialVersionUID = 4490040218923478461L;
    
    private Set<AttrDimension> children = new HashSet<>();
    private final String name;

    public AttrDimension(String name) {
        this.name = name;
    }

    public void setChildren(Collection<AttrDimension> children) {
        this.children = new HashSet<>(children);
    }

    public String getName() {
        return name;
    }

    public AttrDimension clone() {
        AttrDimension dim = new AttrDimension(name);
        List<AttrDimension> children = this.children.stream().map(AttrDimension::clone).collect(Collectors.toList());
        dim.setChildren(children);
        return dim;
    }

    @Override
    public Collection<AttrDimension> getChildren() {
        return children;
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return null;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return !(other == null || !(other instanceof AttrDimension))
                && this.name.equals(((AttrDimension) other).getName());
    }

}
