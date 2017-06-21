package com.latticeengines.leadprioritization.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;

@Component("pivotScoreAndEvent")
public class PivotScoreAndEvent extends TypesafeDataFlowBuilder<PivotScoreAndEventParameters> {

    @Override
    public Node construct(PivotScoreAndEventParameters parameters) {
        Node inputTable = addSource(parameters.getScoreOutputTableName());
        inputTable = inputTable.apply(String.format("%s ? 1 : 0", InterfaceName.Event.name()),
                new FieldList(InterfaceName.Event.name()), new FieldMetadata("IsPositiveEvent", Integer.class));

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsPositiveEvent", "TotalPositiveEvents", AggregationType.SUM));
        aggregations
                .add(new Aggregation(ScoreResultField.Percentile.displayName, "TotalEvents", AggregationType.COUNT));
        Node aggregatedNode = inputTable.groupBy(new FieldList(ScoreResultField.Percentile.displayName), aggregations);

        aggregatedNode = aggregatedNode.apply("TotalEvents == 0 ? 0 : TotalPositiveEvents / TotalEvents",
                new FieldList("TotalPositiveEvents", "TotalEvents"), new FieldMetadata("ConversionRate", Double.class));

        double modelAvgProbability = parameters.getModelAvgProbability();

        String expression = String.format("%s / %f", "ConversionRate", modelAvgProbability);
        aggregatedNode = aggregatedNode.apply(expression, new FieldList("ConversionRate"),
                new FieldMetadata("Lift", Double.class));
        return aggregatedNode;

    }
}
