package com.latticeengines.prospectdiscovery.dataflow;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.CreateScoreTableParameters;

@Component("createScoreTable")
public class CreateScoreTable extends TypesafeDataFlowBuilder<CreateScoreTableParameters> {

    @Override
    public Node construct(CreateScoreTableParameters parameters) {
        Node eventTable = addSource(parameters.getEventTable());
        Node scoreTable = addSource(parameters.getScoreResultsTable());
        
        Node castedScoreTable = scoreTable.addFunction("Long.parseLong(LeadID)", //
                new FieldList("LeadID"), //
                new FieldMetadata("LeadID_Numeric", Long.class));
        return eventTable.innerJoin(parameters.getUniqueKeyColumn(), castedScoreTable, "LeadID_Numeric");
    }
}
