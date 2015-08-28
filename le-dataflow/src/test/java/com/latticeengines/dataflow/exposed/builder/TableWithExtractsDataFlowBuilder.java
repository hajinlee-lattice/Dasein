package com.latticeengines.dataflow.exposed.builder;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("tableWithExtractsDataFlowBuilder")
public class TableWithExtractsDataFlowBuilder extends CascadingDataFlowBuilder {

    public TableWithExtractsDataFlowBuilder() {
        super(true, true);
    }

    /**
     * Load a source table with three extracts of differing schemas.
     */
    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);
        String tableAsJson = sources.get("Source");
        return addSource(JsonUtils.deserialize(tableAsJson, Table.class));
    }

}
