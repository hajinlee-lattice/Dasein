package com.latticeengines.dataflow.exposed.builder;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@Component
public class ExtractFilterBuilder extends TypesafeDataFlowBuilder<DataFlowParameters> {
    @Override
    public Node construct(DataFlowParameters parameters) {
        Node node = addSource("ExtractFilterTest");
        return node;
    }
}
