package com.latticeengines.scoring.dataflow.ev;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePercentile;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

import cascading.tuple.Fields;

@Component("percentileCalculationHelper")
public class PercentileCalculationHelper {

    @Inject
    private NodeSplitter nodeSplitter;

    public Node calculate(ParsedContext context, Node mergedScoreCount, boolean trySecondarySort) {
        Node[] nodes = nodeSplitter.splitEv(mergedScoreCount, context.originalScoreFieldMap,
                context.modelGuidFieldName);
        String secondarySortFieldName = trySecondarySort ? context.outputExpRevFieldName : null;
        Node model = getModel(context, nodes[0], secondarySortFieldName, ScoreResultField.RawScore.displayName);
        Node predictedModel = getModel(context, nodes[1], secondarySortFieldName,
                ScoreResultField.PredictedRevenue.displayName);
        Node evModel = getModel(context, nodes[2], secondarySortFieldName,
                ScoreResultField.ExpectedRevenue.displayName);

        List<Node> models = Arrays.asList(model, predictedModel, evModel).stream().filter(e -> e != null)
                .collect(Collectors.toList());
        Node merged = models.get(0);
        for (int i = 1; i < models.size(); i++) {
            merged = merged.merge(models.get(i));
        }
        return merged;
    }

    private Node getModel(ParsedContext context, Node node, String secondarySortFieldName,
            String originalScoreFieldName) {
        if (node == null) {
            return node;
        }
        return calculatePercentileByFieldName(context.modelGuidFieldName, context.scoreCountFieldName,
                originalScoreFieldName, context.percentileFieldName, secondarySortFieldName, context.minPct,
                context.maxPct, node);
    }

    private Node calculatePercentileByFieldName(String modelGuidFieldName, String scoreCountFieldName,
            String originalScoreFieldName, String percentileFieldName, String secondarySortFieldName, int minPct,
            int maxPct, Node node) {
        if (ScoreResultField.RawScore.displayName.equals(originalScoreFieldName)) {
            return node;
        }

        node = node.addColumnWithFixedValue(percentileFieldName, null, Integer.class);
        List<String> returnedFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> returnedMetadata = new ArrayList<>(node.getSchema());
        FieldList sortFieldList = null;
        if (secondarySortFieldName == null || secondarySortFieldName.equals(originalScoreFieldName)) {
            sortFieldList = new FieldList(originalScoreFieldName);
        } else {
            sortFieldList = new FieldList(originalScoreFieldName, secondarySortFieldName);
        }

        Node calculatePercentile = node.groupByAndBuffer(new FieldList(modelGuidFieldName),
                sortFieldList, new CalculatePercentile(new Fields(returnedFields.toArray(new String[0])), minPct,
                        maxPct, percentileFieldName, scoreCountFieldName, originalScoreFieldName),
                true, returnedMetadata);
        return calculatePercentile;
    }
}
