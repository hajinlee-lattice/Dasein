package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@Component("createPropDataInput")
public class CreatePropDataInput extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);
        addSource("EventTable", sources.get("EventTable"));

        List<Aggregation> aggregation = new ArrayList<>();
        aggregation.add(new Aggregation(Aggregation.AggregationType.FIRST, FieldList.GROUP));

        String groupby = addGroupBy("EventTable", //
                new FieldList("Domain", "Company", "City", "State", "Country", "PropDataHash"), aggregation);
        String withRowId = addRowId(groupby, "RowId", groupby);
        return withRowId;
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }
}
