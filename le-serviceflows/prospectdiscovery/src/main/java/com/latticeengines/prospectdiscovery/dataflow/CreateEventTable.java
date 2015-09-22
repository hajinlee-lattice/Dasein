package com.latticeengines.prospectdiscovery.dataflow;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("createEventTable")
public class CreateEventTable extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);
        addSource("Account", sources.get("Account"));
        addSource("Contact", sources.get("Contact"));
        addSource("Opportunity", sources.get("Opportunity"));

        String account$contact = addJoin("Account", //
                new FieldList("Id"), //
                "Contact", //
                new FieldList("AccountId"), //
                JoinType.INNER);
        
        String oppty$account$contact = addJoin("Opportunity", //
                new FieldList("AccountId"), //
                account$contact, //
                new FieldList("Id"), //
                JoinType.LEFT);
        
        return oppty$account$contact;
    }
}
