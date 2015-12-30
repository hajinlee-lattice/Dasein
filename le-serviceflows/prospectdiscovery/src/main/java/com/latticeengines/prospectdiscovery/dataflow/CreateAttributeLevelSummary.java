package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;

@Component("createAttributeLevelSummary")
public class CreateAttributeLevelSummary extends TypesafeDataFlowBuilder<CreateAttributeLevelSummaryParameters> {

    @Override
    public Node construct(CreateAttributeLevelSummaryParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);

        List<Aggregation> aggregation = new ArrayList<>();

        switch (parameters.aggregationType) {
        case "COUNT":
            aggregation.add(new Aggregation(parameters.aggregateColumn, "Count",
                    Aggregation.AggregationType.COUNT));
            break;

        case "AVG":
            aggregation.add(new Aggregation(parameters.aggregateColumn, "Average",
                    Aggregation.AggregationType.AVG));
            break;

        default:
            break;
        }

        return eventTable.groupBy(new FieldList(parameters.groupByColumns), aggregation);
    }

}
