package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;

@Component("createPropDataInput")
public class CreatePropDataInput extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(Map<String, String> sources) {
        addSource("EventTable", sources.get("EventTable"));

        List<GroupByCriteria> groupByCriteria = new ArrayList<>();
        groupByCriteria.add(new GroupByCriteria(GroupByCriteria.AggregationType.FIRST, FieldList.GROUP));

        String groupby = addGroupBy("EventTable", //
                new FieldList("Domain", "Company", "City", "Country", "PropDataHash"), groupByCriteria);
        String withRowId = addRowId(groupby, "RowId", groupby);
        return withRowId;
    }
}
