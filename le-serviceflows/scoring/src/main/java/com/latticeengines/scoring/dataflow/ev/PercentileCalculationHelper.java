package com.latticeengines.scoring.dataflow.ev;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePercentile;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

import cascading.tuple.Fields;

@Component("percentileCalculationHelper")
public class PercentileCalculationHelper {

    private static final Logger log = LoggerFactory.getLogger(PercentileCalculationHelper.class);

    @Inject
    private NodeSplitter nodeSplitter;

    public Node calculate(ParsedContext context, Node mergedScoreCount, boolean trySecondarySort,
            boolean targetScoreDerivation) {
        Node[] nodes = nodeSplitter.splitEv(mergedScoreCount, context.originalScoreFieldMap,
                context.modelGuidFieldName);
        String secondarySortFieldName = trySecondarySort ? context.outputExpRevFieldName : null;
        Node model = getModel(context, nodes[0], secondarySortFieldName, ScoreResultField.RawScore.displayName,
                targetScoreDerivation);
        Node predictedModel = getModel(context, nodes[1], secondarySortFieldName,
                ScoreResultField.PredictedRevenue.displayName, targetScoreDerivation);
        Node evModel = getModel(context, nodes[2], secondarySortFieldName, ScoreResultField.ExpectedRevenue.displayName,
                targetScoreDerivation);

        List<Node> models = Arrays.asList(model, predictedModel, evModel).stream().filter(e -> e != null)
                .collect(Collectors.toList());
        Node merged = models.get(0);
        for (int i = 1; i < models.size(); i++) {
            merged = merged.merge(models.get(i));
        }
        return merged;
    }

    private Node getModel(ParsedContext context, Node node, String secondarySortFieldName,
            String originalScoreFieldName, boolean targetScoreDerivation) {
        if (node == null) {
            return node;
        }
        return calculatePercentileByFieldName(context.modelGuidFieldName, context.scoreCountFieldName,
                originalScoreFieldName, context.percentileFieldName, context.standardScoreField, secondarySortFieldName,
                context.minPct, context.maxPct, node, context.targetScoreDerivationPaths, context.customerSpace,
                targetScoreDerivation);
    }

    private Node calculatePercentileByFieldName(String modelGuidFieldName, String scoreCountFieldName,
            String originalScoreFieldName, String percentileFieldName, String defaultPercentileFieldName,
            String secondarySortFieldName, int minPct, int maxPct, Node node,
            Map<String, String> targetScoreDerivationPaths, CustomerSpace customerSpace,
            boolean targetScoreDerivation) {
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

        log.info("Use target score derivation=" + targetScoreDerivation);
        Node calculatePercentile = node.groupByAndBuffer(new FieldList(modelGuidFieldName), sortFieldList,
                new CalculatePercentile(new Fields(returnedFields.toArray(new String[0])), minPct, maxPct,
                        percentileFieldName, scoreCountFieldName, originalScoreFieldName, targetScoreDerivation,
                        targetScoreDerivationPaths, modelGuidFieldName),
                true, returnedMetadata);
        return calculatePercentile;
    }
}
