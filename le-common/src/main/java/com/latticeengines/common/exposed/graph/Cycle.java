package com.latticeengines.common.exposed.graph;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Cycle {

    private final List<GraphNode> graphNodes = new ArrayList<GraphNode>();

    public Cycle (List<GraphNode> graphNodes) {
        graphNodes.addAll(graphNodes);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }

        EqualsBuilder eb = new EqualsBuilder();
        Cycle castOther = (Cycle) other;

        for (int i = 0; i < graphNodes.size(); i++) {
            eb.append(graphNodes.get(i), castOther.graphNodes.get(i));
        }
        return eb.isEquals();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();

        for (int i = 0; i < graphNodes.size(); i++) {
            hcb.append(graphNodes.get(i));
        }

        return hcb.toHashCode();
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);

        for (int i = 0; i < graphNodes.size(); i++) {
            tsb.append(graphNodes.get(i));
        }

        return tsb.toString();
    }

    public int size() {
        return graphNodes.size();
    }

    public List<GraphNode> getNodes() {
        return graphNodes;
    }

}