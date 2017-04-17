package com.latticeengines.domain.exposed.dependency;

import java.util.List;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.domain.exposed.metadata.DependableType;

public interface Dependable extends GraphNode {
    DependableType getDependableType();

    List<? extends Dependable> getDependencies();

    String getDependableName();
}
