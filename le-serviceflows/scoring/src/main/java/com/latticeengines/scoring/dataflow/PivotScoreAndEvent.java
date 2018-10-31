package com.latticeengines.scoring.dataflow;

import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.BUCKET_AVG_SCORE;
import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.BUCKET_LIFT;
import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.BUCKET_SUM;
import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.BUCKET_TOTAL_EVENTS;
import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.BUCKET_TOTAL_POSITIVE_EVENTS;
import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.MODEL_AVG;
import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.MODEL_GUID;
import static com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils.MODEL_SUM;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePositiveEventsFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;

@Component("pivotScoreAndEvent")
public class PivotScoreAndEvent extends TypesafeDataFlowBuilder<PivotScoreAndEventParameters> {

    private static final Logger log = LoggerFactory.getLogger(PivotScoreAndEvent.class);

    private Node total;
    private boolean useEvent;

    @Override
    public Node construct(PivotScoreAndEventParameters parameters) {
        Node inputTable = addSource(parameters.getScoreOutputTableName());

        Map<String, Double> avgScoresMap = parameters.getAvgScores();
        Map<String, String> scoreFieldMap = parameters.getScoreFieldMap();
        Map<String, Node> nodes = splitNodes(inputTable, scoreFieldMap);

        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            String scoreField = scoreFieldMap.getOrDefault(modelGuid, InterfaceName.RawScore.name());
            useEvent = InterfaceName.Event.name().equals(scoreField);
            boolean isEV = InterfaceName.ExpectedRevenue.name().equals(scoreField);
            log.info("useEvent=" + useEvent + " isEV=" + isEV + " ModeId=" + modelGuid);

            Node node = entry.getValue();
            total = getTotal(node, modelGuid, scoreField, isEV);
            Node aggregatedNode = aggregate(node, scoreField, isEV);
            String scoreDerivation = parameters.getScoreDerivationMap().get(modelGuid);
            String fitFunctionParams = parameters.getFitFunctionParametersMap().get(modelGuid);

            Double avgScore = avgScoresMap.get(modelGuid);
            Node output = createLift(aggregatedNode, avgScore, scoreDerivation, fitFunctionParams, isEV);
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

    private Node getTotal(Node node, String modelGuid, String scoreField, boolean isEV) {
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

    private Node aggregate(Node inputTable, String scoreField, boolean isEV) {
        List<Aggregation> aggregations = new ArrayList<>();
        String percentileScoreField = isEV ? ScoreResultField.ExpectedRevenuePercentile.displayName
                : ScoreResultField.Percentile.displayName;
        aggregations.add(new Aggregation(percentileScoreField, BUCKET_TOTAL_EVENTS, AggregationType.COUNT));
        if (useEvent) {
            inputTable = inputTable.apply(String.format("Boolean.TRUE.equals(%s) ? 1 : 0", InterfaceName.Event.name()),
                    new FieldList(InterfaceName.Event.name()), new FieldMetadata("IsPositiveEvent", Integer.class));
            aggregations.add(new Aggregation("IsPositiveEvent", BUCKET_TOTAL_POSITIVE_EVENTS, AggregationType.SUM));
        } else {
            aggregations.add(new Aggregation(scoreField, BUCKET_AVG_SCORE, AggregationType.AVG));
            aggregations.add(new Aggregation(scoreField, BUCKET_SUM, AggregationType.SUM));
        }
        Node aggregatedNode = inputTable
                .groupBy(new FieldList(percentileScoreField, ScoreResultField.ModelId.displayName), aggregations);
        return aggregatedNode;
    }

    private Node createLift(Node aggregatedNode, Double avgScore, String scoreDerivation, String fitFunctionParams,
            boolean isEV) {
        if (useEvent) {
            double modelAvgProbability = avgScore;
            String expression = String.format("%s / %f", "ConversionRate", modelAvgProbability);
            aggregatedNode = aggregatedNode.apply(
                    String.format("%1$s == 0 ? 0 : %2$s / %1$s", BUCKET_TOTAL_EVENTS, BUCKET_TOTAL_POSITIVE_EVENTS),
                    new FieldList(BUCKET_TOTAL_POSITIVE_EVENTS, BUCKET_TOTAL_EVENTS),
                    new FieldMetadata("ConversionRate", Double.class));
            aggregatedNode = aggregatedNode.apply(expression, new FieldList("ConversionRate"),
                    new FieldMetadata(BUCKET_LIFT, Double.class));
        } else {
            aggregatedNode = aggregatedNode.innerJoin(MODEL_GUID, total, MODEL_GUID);
            aggregatedNode = aggregatedNode.apply(
                    String.format("%1$s > 0 ? %2$s / %1$s : 0.0", MODEL_AVG, BUCKET_AVG_SCORE),
                    new FieldList(BUCKET_AVG_SCORE, MODEL_AVG), new FieldMetadata(BUCKET_LIFT, Double.class));

            if (!isEV) {
                aggregatedNode = getTotalPositiveEvents(aggregatedNode, scoreDerivation, fitFunctionParams, isEV);
                aggregatedNode = aggregatedNode.addColumnWithFixedValue(
                        ScoreResultField.ExpectedRevenuePercentile.displayName, null, Integer.class);
                aggregatedNode = aggregatedNode.addColumnWithFixedValue(BUCKET_AVG_SCORE, null, Double.class);
                aggregatedNode = aggregatedNode.addColumnWithFixedValue(BUCKET_SUM, null, Double.class);
                aggregatedNode = aggregatedNode.retain(ScoreResultField.ModelId.displayName,
                        ScoreResultField.Percentile.displayName, ScoreResultField.ExpectedRevenuePercentile.displayName,
                        BUCKET_TOTAL_POSITIVE_EVENTS, BUCKET_TOTAL_EVENTS, BUCKET_LIFT, BUCKET_AVG_SCORE, BUCKET_SUM);
                log.info("non EV aggregatedNode fields = " + JsonUtils.serialize(aggregatedNode.getFieldNames()));
            } else {
                String expression = String.format("%1$s == 0 ? 0 : (%1$s * %2$s / %3$s)", BUCKET_TOTAL_EVENTS,
                        BUCKET_SUM, MODEL_SUM);
                aggregatedNode = aggregatedNode.apply(expression,
                        new FieldList(BUCKET_SUM, BUCKET_TOTAL_EVENTS, MODEL_SUM),
                        new FieldMetadata(BUCKET_TOTAL_POSITIVE_EVENTS, Double.class));
                aggregatedNode = aggregatedNode.addColumnWithFixedValue(ScoreResultField.Percentile.displayName, null,
                        Integer.class);
                aggregatedNode = aggregatedNode.retain(ScoreResultField.ModelId.displayName,
                        ScoreResultField.Percentile.displayName, ScoreResultField.ExpectedRevenuePercentile.displayName,
                        BUCKET_TOTAL_POSITIVE_EVENTS, BUCKET_TOTAL_EVENTS, BUCKET_LIFT, BUCKET_AVG_SCORE, BUCKET_SUM);
                log.info("EV aggregatedNode fields = " + JsonUtils.serialize(aggregatedNode.getFieldNames()));
            }
        }
        return aggregatedNode;
    }

    private Node getTotalPositiveEvents(Node aggregatedNode, String scoreDerivation, String fitFunctionParams,
            boolean isEV) {
        if (scoreDerivation == null || fitFunctionParams == null) {
            return getTotalPositiveEventsUsingAvgScore(aggregatedNode);
        } else {
            return getTotalPositiveEventsUsingFitFunction(aggregatedNode, scoreDerivation, fitFunctionParams, isEV);
        }
    }

    private Node getTotalPositiveEventsUsingFitFunction(Node aggregatedNode, String scoreDerivation,
            String fitFunctionParams, boolean isEV) {
        return aggregatedNode.apply(
                new CalculatePositiveEventsFunction(BUCKET_TOTAL_POSITIVE_EVENTS, BUCKET_AVG_SCORE, BUCKET_TOTAL_EVENTS,
                        scoreDerivation, fitFunctionParams, isEV),
                new FieldList(BUCKET_AVG_SCORE, BUCKET_TOTAL_EVENTS),
                new FieldMetadata(BUCKET_TOTAL_POSITIVE_EVENTS, Double.class));
    }

    private Node getTotalPositiveEventsUsingAvgScore(Node aggregatedNode) {
        String expression = String.format("%1$s == 0 ? 0 : (%2$s * %1$s)", BUCKET_TOTAL_EVENTS, BUCKET_AVG_SCORE);
        aggregatedNode = aggregatedNode.apply(expression, new FieldList(BUCKET_AVG_SCORE, BUCKET_TOTAL_EVENTS),
                new FieldMetadata(BUCKET_TOTAL_POSITIVE_EVENTS, Double.class));
        return aggregatedNode;
    }

}
