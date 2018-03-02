package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;

@Component("pivotScoreAndEvent")
public class PivotScoreAndEvent extends TypesafeDataFlowBuilder<PivotScoreAndEventParameters> {

    private static final String AVG_SCORE = "AverageScore";
    private static final String AVG_REVENUE = "AverageRevenue";
    private static final String TOTAL_EVENTS = "TotalEvents";
    private static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";
    private static final String LIFT = "Lift";

    @Override
    public Node construct(PivotScoreAndEventParameters parameters) {
        Node inputTable = addSource(parameters.getScoreOutputTableName());

        Map<String, Double> avgScoresMap = parameters.getAvgScores();
        Map<String, Boolean> isExpectedValueMap = parameters.getExpectedValues();
        Map<String, Node> nodes = splitNodes(inputTable, avgScoresMap);

        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();
            double avgScore = avgScoresMap.get(modelGuid);
            boolean isEV = isExpectedValueMap.get(modelGuid);
            boolean isCDL = node.getFieldNames().contains(InterfaceName.NormalizedScore.name());
            Node aggregatedNode = aggregate(node, isCDL, isEV);
            Node output = createLift(aggregatedNode, isCDL, avgScore, isEV);
            if (merged == null) {
                merged = output;
            } else {
                merged = merged.merge(output);
            }
        }
        return merged;

    }

    private Map<String, Node> splitNodes(Node input, Map<String, Double> avgScoresMap) {
        Map<String, Node> nodes = new HashMap<>();
        avgScoresMap.forEach((modelGuid, avgScore) -> {
            Node model = input.filter(
                    String.format("\"%s\".equals(%s)", modelGuid, ScoreResultField.ModelId.displayName),
                    new FieldList(ScoreResultField.ModelId.displayName));
            model = model.renamePipe(modelGuid);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    private Node aggregate(Node inputTable, boolean isCDL, boolean isEV) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(ScoreResultField.Percentile.displayName, TOTAL_EVENTS, AggregationType.COUNT));
        if (!isCDL) {
            inputTable = inputTable.apply(String.format("%s ? 1 : 0", InterfaceName.Event.name()),
                    new FieldList(InterfaceName.Event.name()), new FieldMetadata("IsPositiveEvent", Integer.class));
            aggregations.add(new Aggregation("IsPositiveEvent", TOTAL_POSITIVE_EVENTS, AggregationType.SUM));
        } else {
            aggregations.add(new Aggregation(InterfaceName.NormalizedScore.name(), AVG_SCORE, AggregationType.AVG));
            if (isEV) {
                aggregations
                        .add(new Aggregation(InterfaceName.ExpectedRevenue.name(), AVG_REVENUE, AggregationType.AVG));
            }
        }

        Node aggregatedNode = inputTable.groupBy(new FieldList(ScoreResultField.Percentile.displayName), aggregations);
        return aggregatedNode;
    }

    private Node createLift(Node aggregatedNode, boolean isCDL, double avgScore, boolean isEV) {
        if (!isCDL) {
            double modelAvgProbability = avgScore;
            String expression = String.format("%s / %f", "ConversionRate", modelAvgProbability);
            aggregatedNode = aggregatedNode.apply(
                    String.format("%1$s == 0 ? 0 : %2$s / %1$s", TOTAL_EVENTS, TOTAL_POSITIVE_EVENTS),
                    new FieldList(TOTAL_POSITIVE_EVENTS, TOTAL_EVENTS),
                    new FieldMetadata("ConversionRate", Double.class));
            aggregatedNode = aggregatedNode.apply(expression, new FieldList("ConversionRate"),
                    new FieldMetadata(LIFT, Double.class));
        } else {
            String expression = String.format("%1$s == 0 ? 0 : (%2$s * 0.01 * %1$s)", TOTAL_EVENTS, AVG_SCORE);
            aggregatedNode = aggregatedNode.apply(expression, new FieldList(AVG_SCORE, TOTAL_EVENTS),
                    new FieldMetadata(TOTAL_POSITIVE_EVENTS, Double.class));

            String scoreFieldName = AVG_SCORE;
            expression = String.format("%s * 0.01 / %f", scoreFieldName, avgScore);
            if (isEV) {
                scoreFieldName = AVG_REVENUE;
                expression = String.format("%s / %f", scoreFieldName, avgScore);
            }
            aggregatedNode = aggregatedNode.apply(expression, new FieldList(scoreFieldName),
                    new FieldMetadata(LIFT, Double.class));
        }
        aggregatedNode = aggregatedNode.retain(ScoreResultField.Percentile.displayName, TOTAL_POSITIVE_EVENTS,
                TOTAL_EVENTS, LIFT);
        return aggregatedNode;
    }

}
