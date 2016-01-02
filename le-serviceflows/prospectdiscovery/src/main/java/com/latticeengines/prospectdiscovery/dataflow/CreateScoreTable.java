package com.latticeengines.prospectdiscovery.dataflow;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.Aggregation.AggregationType;
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
        Node joinTable = eventTable.innerJoin(parameters.getUniqueKeyColumn(), castedScoreTable, "LeadID_Numeric");
        Aggregation aggregation = new Aggregation("Probability", "AverageProbability", AggregationType.AVG);
        return joinTable.aggregate(aggregation);
    }
}
