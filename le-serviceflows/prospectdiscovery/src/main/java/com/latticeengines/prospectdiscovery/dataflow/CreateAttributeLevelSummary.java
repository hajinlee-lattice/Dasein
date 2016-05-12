package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;

@Component("createAttributeLevelSummary")
public class CreateAttributeLevelSummary extends TypesafeDataFlowBuilder<CreateAttributeLevelSummaryParameters> {

    @Override
    public Node construct(CreateAttributeLevelSummaryParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);

        List<Aggregation> aggregation = new ArrayList<>();
        switch (parameters.aggregationType) {
        case "COUNT":
            aggregation.add(new Aggregation(parameters.aggregateColumn, "Count", AggregationType.COUNT));
            return eventTable.groupBy(new FieldList(parameters.groupByColumns), aggregation);

        default:
            aggregation.add(new Aggregation(parameters.aggregateColumn, "Average", AggregationType.AVG));
            Node aggregated = eventTable.groupBy(new FieldList(parameters.groupByColumns), aggregation);
            return aggregated.addFunction("Average/AverageProbability", new FieldList("Average", "AverageProbability"), //
                    new FieldMetadata("Lift", Double.class));

        }

    }

}
