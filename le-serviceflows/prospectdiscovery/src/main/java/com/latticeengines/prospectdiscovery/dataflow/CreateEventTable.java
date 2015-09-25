package com.latticeengines.prospectdiscovery.dataflow;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("createEventTable")
public class CreateEventTable extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, //
            Map<String, String> sources, //
            Map<String, Table> sourceTables) { 
        setDataFlowCtx(dataFlowCtx);
        String account = addSource(sourceTables.get("Account"));
        String contact = addSource(sourceTables.get("Contact"));
        String oppty = addSource(sourceTables.get("Opportunity"));

        String account$contact = addJoin(account, //
                new FieldList("Id"), //
                contact, //
                new FieldList("AccountId"), //
                JoinType.INNER);
        
        String oppty$account$contact = addJoin(oppty, //
                new FieldList("AccountId"), //
                account$contact, //
                new FieldList("Id"), //
                JoinType.LEFT);
        
        return oppty$account$contact;
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        return null;
    }
}
