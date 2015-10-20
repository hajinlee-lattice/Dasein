package com.latticeengines.dataflow.exposed.builder;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("tableWithExtractsDataFlowBuilder")
public class TableWithExtractsDataFlowBuilder extends CascadingDataFlowBuilder {

    /**
     * Load a source table with three extracts of differing schemas.
     */
    @Override
    public Node constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources, Map<String, Table> sourceTables) {
        setDataFlowCtx(dataFlowCtx);
        return addSource(sourceTables.get("Source"));
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        return null;
    }

}
