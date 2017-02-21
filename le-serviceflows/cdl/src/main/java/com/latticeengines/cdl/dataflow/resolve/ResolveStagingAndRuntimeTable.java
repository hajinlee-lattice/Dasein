package com.latticeengines.cdl.dataflow.resolve;

import org.springframework.stereotype.Component;

import com.latticeengines.cdl.dataflow.TypesafeDataFlowWithResolutionBuilder;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.FieldLoadStrategy;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.KeyLoadStrategy;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.ResolveStagingAndRuntimeTableParameters;

@Component("resolveStagingAndRuntimeTable")
public class ResolveStagingAndRuntimeTable extends TypesafeDataFlowWithResolutionBuilder<ResolveStagingAndRuntimeTableParameters> {

    @Override
    public Node construct(ResolveStagingAndRuntimeTableParameters parameters) {
        FieldLoadStrategy fieldLoadStrategy = parameters.fieldLoadStrategy;
        KeyLoadStrategy keyLoadStrategy = parameters.keyLoadStrategy;
        
        
        return null;
    }

}
