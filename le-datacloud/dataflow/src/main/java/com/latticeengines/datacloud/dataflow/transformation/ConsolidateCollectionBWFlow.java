package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;

@Component("consolidateCollectionBWFlow")
public class ConsolidateCollectionBWFlow extends ConsolidateCollectionFlow {
    public static final String BEAN_NAME = "consolidateCollectionBWFlow";

    @Override
    protected Node preRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        return src;
    }

    @Override
    protected Node postRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        return src;
    }
}
