package com.latticeengines.scoring.dataflow.ev;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.LookupPercentileForRevenueFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil.ScoreDerivationType;

import cascading.operation.aggregator.Average;
import cascading.tuple.Fields;

@Component("helper")
public class Helper {

    @Inject
    public Configuration yarnConfiguration;

    @Inject
    public ModelSummaryProxy modelSummaryProxy;

    public Map<String, Node> splitNodes(Node input, Map<String, String> originalScoreFieldMap,
            String modelGuidFieldName) {
        Map<String, Node> nodes = new HashMap<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            Node model = input.filter(String.format("\"%s\".equals(%s)", modelGuid, modelGuidFieldName),
                    new FieldList(ScoreResultField.ModelId.displayName));
            model = model.renamePipe(modelGuid);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    public Node doSomethingHere(ParsedContext context, Node calculatePercentile) {
        Node node = calculateAverageEV(context, calculatePercentile);
        node = lookupPercentileForAverageEV(context, node);
        return node;
    }

    private Node calculateAverageEV(ParsedContext context, Node calculatePercentile) {
        // 4. for each percentile bucket calculate average expected
        // revenue
        FieldList outputFields = new FieldList("PercentileField", "expectedRevenue");
        Node result = calculatePercentile.retain(outputFields);
        FieldList groupByFieldList = new FieldList(context.modelGuidFieldName);
        String scoreFieldName = ScoreResultField.Percentile.displayName;
        // Node score = node.retain(scoreFieldName,
        // modelGuidFieldName).renamePipe(scoreCountPipeName);
        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                new FieldMetadata(context.modelGuidFieldName, String.class), //
                new FieldMetadata(scoreFieldName, String.class), //
                new FieldMetadata(context.scoreCountFieldName, Long.class));
        result.groupByAndAggregate(groupByFieldList, new Average(new Fields(context.scoreCountFieldName)), //
                scoreCountFms, Fields.ALL);
        // calculate average expected revenue for modelId-percentile pair (for
        // EV models) and then join this column back to actual data
        // then use the fitter fuction to go throw each record to lookup
        // temporary EV percentile score for average expected revenue value
        // and then use this percentile score to use fit function and comeup
        // with fitted EV value
        return calculatePercentile;
    }

    private Node lookupPercentileForAverageEV(ParsedContext context, Node node) {
        // 5. Use "ev" field from evScoreDerivation.json to lookup for
        // temporary EV percentile score for average expected revenue
        // value
        if (MapUtils.isNotEmpty(context.originalScoreFieldMap)) {
            node = node.addColumnWithFixedValue(context.percentileFieldName + "__temp__", null, Integer.class);
            FieldList retainedFields = new FieldList(node.getFieldNames());

            Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMap = ExpectedRevenueDataFlowUtil
                    .getScoreDerivationMap(context.customerSpace, yarnConfiguration, modelSummaryProxy,
                            context.originalScoreFieldMap, true);

            node = lookupPercentileFromScoreDerivation(scoreDerivationMap, context.originalScoreFieldMap,
                    context.modelGuidFieldName, context.percentileFieldName + "__temp__", context.expectedRevenueField,
                    context.minPct, context.maxPct, node);
            node = node.retain(retainedFields);
            return node;
        }

        return node;
    }

    private Node lookupPercentileFromScoreDerivation(
            Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMap, //
            Map<String, String> originalScoreFieldMap, //
            String modelGuidFieldName, String percentileFieldName, String revenueFieldName, int minPct, int maxPct,
            Node mergedScoreCount) {

        Map<String, Node> nodes = splitNodes(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            if (scoreDerivationMap.containsKey(modelGuid)) {
                node = node.apply(
                        new LookupPercentileForRevenueFunction( //
                                new Fields(node.getFieldNamesArray()), revenueFieldName, percentileFieldName,
                                scoreDerivationMap.get(modelGuid).get(ScoreDerivationType.REVENUE)),
                        new FieldList(node.getFieldNamesArray()), node.getSchema(), null, Fields.REPLACE);
            }

            if (merged == null) {
                merged = node;
            } else {
                merged = merged.merge(node);
            }
        }
        return merged;
    }
}
