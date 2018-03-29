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
    private static final String AVG_SUM = "AverageSum";

    private static final String TOTAL_EVENTS = "TotalEvents";
    private static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";
    private static final String LIFT = "Lift";
    private static final String MODEL_GUID = ScoreResultField.ModelId.displayName;

    public static final String MODEL_AVG = "ModelAvg";
    private static final String MODEL_SUM = "ModelSum";

    private Node total;
    // private Node totalSum;
    private boolean useEvent;
    private boolean isEV;

    @Override
    public Node construct(PivotScoreAndEventParameters parameters) {
        Node inputTable = addSource(parameters.getScoreOutputTableName());

        Map<String, Double> avgScoresMap = parameters.getAvgScores();
        Map<String, String> scoreFieldMap = parameters.getScoreFieldMap();
        Map<String, Node> nodes = splitNodes(inputTable, scoreFieldMap);

        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            Double avgScore = avgScoresMap.get(modelGuid);
            String scoreField = scoreFieldMap.getOrDefault(modelGuid, InterfaceName.RawScore.name());
            useEvent = InterfaceName.Event.name().equals(scoreField);
            isEV = InterfaceName.ExpectedRevenue.name().equals(scoreField);

            total = getTotal(node, modelGuid, scoreField);
            Node aggregatedNode = aggregate(node, scoreField);

            Node output = createLift(aggregatedNode, avgScore);
            if (merged == null) {
                merged = output;
            } else {
                merged = merged.merge(output);
            }
        }
        return merged;

    }

    private Map<String, Node> splitNodes(Node input, Map<String, String> scoreFieldMap) {
        Map<String, Node> nodes = new HashMap<>();
        scoreFieldMap.forEach((modelGuid, scoreField) -> {
            Node model = input.filter(String.format("\"%s\".equals(%s)", modelGuid, MODEL_GUID),
                    new FieldList(ScoreResultField.ModelId.displayName));
            model = model.renamePipe(modelGuid);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    private Node getTotal(Node node, String modelGuid, String scoreField) {
        if (useEvent) {
            node = node.apply(String.format("Boolean.TRUE.equals(%s) ? 1.0 : 0.0", scoreField),
                    new FieldList(scoreField), new FieldMetadata("EventScore", Double.class));
            scoreField = "EventScore";
        }
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(scoreField, MODEL_AVG, AggregationType.AVG));
        if (isEV) {
            aggregations.add(new Aggregation(scoreField, MODEL_SUM, AggregationType.SUM));
        }
        Node total = node.retain(scoreField, MODEL_GUID) //
                .groupBy(new FieldList(MODEL_GUID), aggregations);
        total = total.renamePipe(modelGuid + "_total");
        return total;
    }

    private Node aggregate(Node inputTable, String scoreField) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(ScoreResultField.Percentile.displayName, TOTAL_EVENTS, AggregationType.COUNT));
        if (useEvent) {
            inputTable = inputTable.apply(String.format("Boolean.TRUE.equals(%s) ? 1 : 0", InterfaceName.Event.name()),
                    new FieldList(InterfaceName.Event.name()), new FieldMetadata("IsPositiveEvent", Integer.class));
            aggregations.add(new Aggregation("IsPositiveEvent", TOTAL_POSITIVE_EVENTS, AggregationType.SUM));
        } else {
            aggregations.add(new Aggregation(scoreField, AVG_SCORE, AggregationType.AVG));
            if (isEV) {
                aggregations.add(new Aggregation(scoreField, AVG_SUM, AggregationType.SUM));
            }
        }
        Node aggregatedNode = inputTable.groupBy(
                new FieldList(ScoreResultField.Percentile.displayName, ScoreResultField.ModelId.displayName),
                aggregations);
        return aggregatedNode;
    }

    private Node createLift(Node aggregatedNode, Double avgScore) {
        if (useEvent) {
            double modelAvgProbability = avgScore;
            String expression = String.format("%s / %f", "ConversionRate", modelAvgProbability);
            aggregatedNode = aggregatedNode.apply(
                    String.format("%1$s == 0 ? 0 : %2$s / %1$s", TOTAL_EVENTS, TOTAL_POSITIVE_EVENTS),
                    new FieldList(TOTAL_POSITIVE_EVENTS, TOTAL_EVENTS),
                    new FieldMetadata("ConversionRate", Double.class));
            aggregatedNode = aggregatedNode.apply(expression, new FieldList("ConversionRate"),
                    new FieldMetadata(LIFT, Double.class));
        } else {
            aggregatedNode = aggregatedNode.innerJoin(MODEL_GUID, total, MODEL_GUID);
            aggregatedNode = aggregatedNode.apply(String.format("%1$s > 0 ? %2$s / %1$s : 0.0", MODEL_AVG, AVG_SCORE),
                    new FieldList(AVG_SCORE, MODEL_AVG), new FieldMetadata(LIFT, Double.class));

            if (!isEV) {
                String expression = String.format("%1$s == 0 ? 0 : (%2$s * %1$s)", TOTAL_EVENTS, AVG_SCORE);
                aggregatedNode = aggregatedNode.apply(expression, new FieldList(AVG_SCORE, TOTAL_EVENTS),
                        new FieldMetadata(TOTAL_POSITIVE_EVENTS, Double.class));
            } else {
                String expression = String.format("%1$s == 0 ? 0 : (%1$s * %2$s / %3$s)", TOTAL_EVENTS, AVG_SUM,
                        MODEL_SUM);
                aggregatedNode = aggregatedNode.apply(expression, new FieldList(AVG_SUM, TOTAL_EVENTS, MODEL_SUM),
                        new FieldMetadata(TOTAL_POSITIVE_EVENTS, Double.class));
            }
        }
        aggregatedNode = aggregatedNode.retain(ScoreResultField.ModelId.displayName,
                ScoreResultField.Percentile.displayName, TOTAL_POSITIVE_EVENTS, TOTAL_EVENTS, LIFT);
        return aggregatedNode;
    }

}
