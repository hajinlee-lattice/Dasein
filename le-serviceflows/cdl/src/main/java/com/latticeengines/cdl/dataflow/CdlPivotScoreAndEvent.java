package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.AddNormalizedProbabilityColumnFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CdlPivotScoreAndEventParameters;

@Component("cdlPivotScoreAndEvent")
public class CdlPivotScoreAndEvent extends TypesafeDataFlowBuilder<CdlPivotScoreAndEventParameters> {

    private static final String AVG_PROBILITY = "AverageProbability";
    private static final String AVG_REVENUE = "AverageRevenue";

    @Override
    public Node construct(CdlPivotScoreAndEventParameters parameters) {

        Node result = normalizeProbility(parameters);
        result = aggregate(parameters, result);
        result = createLift(parameters, result);
        return result;

    }

    private Node normalizeProbility(CdlPivotScoreAndEventParameters parameters) {
        Node inputTable = addSource(parameters.getScoreOutputTableName());
        Node result = inputTable.discard(new FieldList(ScoreResultField.Percentile.displayName));
        result = result.apply(
                new AddNormalizedProbabilityColumnFunction(InterfaceName.Probability.name(),
                        ScoreResultField.Percentile.displayName),
                new FieldList(InterfaceName.Probability.name()),
                new FieldMetadata(ScoreResultField.Percentile.displayName, Integer.class));
        return result;
    }

    private Node createLift(CdlPivotScoreAndEventParameters parameters, Node result) {
        String expression = String.format("TotalEvents == 0 ? 0 : (%s * TotalEvents)", AVG_PROBILITY);
        result = result.apply(expression, new FieldList(AVG_PROBILITY, "TotalEvents"),
                new FieldMetadata("TotalPositiveEvents", Double.class));

        String scoreFieldName = parameters.isExpectedValue() ? AVG_REVENUE : AVG_PROBILITY;
        expression = String.format("%s / %f", scoreFieldName, parameters.getAvgScore());
        result = result.apply(expression, new FieldList(scoreFieldName), new FieldMetadata("Lift", Double.class));
        return result;
    }

    private Node aggregate(CdlPivotScoreAndEventParameters parameters, Node result) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(InterfaceName.Probability.name(), AVG_PROBILITY, AggregationType.AVG));
        if (parameters.isExpectedValue()) {
            aggregations.add(new Aggregation(InterfaceName.ExpectedRevenue.name(), AVG_REVENUE, AggregationType.AVG));
        }
        aggregations
                .add(new Aggregation(ScoreResultField.Percentile.displayName, "TotalEvents", AggregationType.COUNT));
        result = result.groupBy(new FieldList(ScoreResultField.Percentile.displayName), aggregations);
        return result;
    }
}
