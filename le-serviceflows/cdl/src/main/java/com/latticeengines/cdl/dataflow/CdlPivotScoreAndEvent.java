package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CdlPivotScoreAndEventParameters;

@Component("cdlPivotScoreAndEvent")
public class CdlPivotScoreAndEvent extends TypesafeDataFlowBuilder<CdlPivotScoreAndEventParameters> {

    private static final String AVG_SCORE = "AverageScore";
    private static final String AVG_REVENUE = "AverageRevenue";

    @Override
    public Node construct(CdlPivotScoreAndEventParameters parameters) {
        Node result = aggregate(parameters);
        result = createLift(parameters, result);
        return result;

    }

    private Node aggregate(CdlPivotScoreAndEventParameters parameters) {
        Node result = addSource(parameters.getScoreOutputTableName());
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(InterfaceName.NormalizedScore.name(), AVG_SCORE, AggregationType.AVG));
        if (parameters.isExpectedValue()) {
            aggregations.add(new Aggregation(InterfaceName.ExpectedRevenue.name(), AVG_REVENUE, AggregationType.AVG));
        }
        aggregations
                .add(new Aggregation(ScoreResultField.Percentile.displayName, "TotalEvents", AggregationType.COUNT));
        result = result.groupBy(new FieldList(ScoreResultField.Percentile.displayName), aggregations);
        return result;
    }

    private Node createLift(CdlPivotScoreAndEventParameters parameters, Node result) {
        String expression = String.format("TotalEvents == 0 ? 0 : (%s * 0.01 * TotalEvents)", AVG_SCORE);
        result = result.apply(expression, new FieldList(AVG_SCORE, "TotalEvents"),
                new FieldMetadata("TotalPositiveEvents", Double.class));

        String scoreFieldName = AVG_SCORE;
        expression = String.format("%s * 0.01 / %f", scoreFieldName, parameters.getAvgScore());
        if (parameters.isExpectedValue()) {
            scoreFieldName = AVG_REVENUE;
            expression = String.format("%s / %f", scoreFieldName, parameters.getAvgScore());
        }
        result = result.apply(expression, new FieldList(scoreFieldName), new FieldMetadata("Lift", Double.class));
        return result;
    }

}
