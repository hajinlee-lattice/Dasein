package com.latticeengines.dataflow.exposed.builder;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@Component("tableWithExtractsDataFlowBuilder")
public class TableWithExtractsDataFlowBuilder extends CascadingDataFlowBuilder {

    /**
     * Load a source table with three extracts of differing schemas.
     */
    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        return addSource("Source");
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        return null;
    }

}
