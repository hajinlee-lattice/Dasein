package com.latticeengines.common.exposed.graph;

import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Cycle {

    private final List<GraphNode> graphNodes;

    public Cycle (List<GraphNode> graphNodes) {
        this.graphNodes = graphNodes;
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

        Cycle castOther = (Cycle) other;

        if (this.getNodes().size() != castOther.getNodes().size()) return false;

        int size = this.getNodes().size();
        int commonHead = castOther.getNodes().indexOf(this.getNodes().get(0));
        if (commonHead == -1) return false;

        for (int i = 0; i < graphNodes.size(); i++) {
            GraphNode thisNode = graphNodes.get(i);
            GraphNode thatNode = castOther.getNodes().get((i + commonHead) % size);
            if (!thisNode.equals(thatNode)) return false;
        }
        return true;
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
