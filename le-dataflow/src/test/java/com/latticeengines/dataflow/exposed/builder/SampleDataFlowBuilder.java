package com.latticeengines.dataflow.exposed.builder;

import java.util.Map;

import org.springframework.stereotype.Component;

import cascading.flow.FlowDef;

@Component("sampleDataFlowBuilder")
public class SampleDataFlowBuilder extends CascadingDataFlowBuilder {
    
    public SampleDataFlowBuilder() {
        super(true);
    }

    @Override
    public FlowDef constructFlowDefinition(Map<String, String> sources) {
        return null;
    }

}
