package com.latticeengines.scoring.dataflow.ev;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.LookupPercentileForRevenueFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters.ScoreDerivationType;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;

import cascading.operation.aggregator.Average;
import cascading.tuple.Fields;

@Component("percentileLookupAverageEvHelper")
public class PercentileLookupAverageEvHelper {
    private static final Logger log = LoggerFactory.getLogger(PercentileLookupAverageEvHelper.class);

    @Inject
    public Configuration yarnConfiguration;

    @Inject
    public ModelSummaryProxy modelSummaryProxy;

    @Inject
    private NodeSplitter nodeSplitter;

    public Node calculate(ParsedContext context, Node calculatePercentile) {
        Node node = calculateAverageEV(context, calculatePercentile);
        return lookupPercentileForAverageEV(context, node);
    }

    private Node calculateAverageEV(ParsedContext context, Node calculatePercentile) {
        Node result = calculatePercentile.renamePipe("calculateAverageEV_" + System.currentTimeMillis());
        result = result.retain(context.expectedRevenueField, context.modelGuidFieldName, context.percentileFieldName);

        FieldList groupByFieldList = new FieldList(context.modelGuidFieldName, context.percentileFieldName);

        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                new FieldMetadata(context.outputExpRevFieldName, Double.class), //
                new FieldMetadata(context.expectedRevenueField, Double.class), //
                new FieldMetadata(context.modelGuidFieldName, String.class), //
                new FieldMetadata(context.percentileFieldName, Integer.class)); //
        log.info(String.format("result fields = %s", JsonUtils.serialize(result.getFieldNames())));

        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps = context.scoreDerivationMaps;
        if (MapUtils.isEmpty(context.scoreDerivationMaps)) {
            scoreDerivationMaps = ExpectedRevenueDataFlowUtil.getScoreDerivationMap(context.customerSpace,
                    yarnConfiguration, modelSummaryProxy, context.originalScoreFieldMap, context.expectedRevenueField,
                    true);
            context.scoreDerivationMaps = scoreDerivationMaps;
        }

        Map<String, Node> nodes = nodeSplitter.split(result, context.originalScoreFieldMap, context.modelGuidFieldName);
        Node aggregatedNode = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            node = node.groupByAndAggregate(groupByFieldList, new Average(new Fields(context.outputExpRevFieldName)), //
                    scoreCountFms, Fields.ALL);
            if (!context.scoreDerivationMaps.containsKey(modelGuid)
                    || !context.scoreDerivationMaps.get(modelGuid).containsKey(ScoreDerivationType.EV)) {
                node = node.discard(context.outputExpRevFieldName);
                node = node.addColumnWithFixedValue(context.outputExpRevFieldName, null, Double.class);
            }

            node = node.retain(context.modelGuidFieldName, context.percentileFieldName, context.outputExpRevFieldName);

            if (aggregatedNode == null) {
                aggregatedNode = node;
            } else {
                aggregatedNode = aggregatedNode.merge(node);
            }
        }
        aggregatedNode = aggregatedNode.renamePipe("aggr_" + System.currentTimeMillis());

        log.info(String.format("aggregatedNode fields = %s", JsonUtils.serialize(aggregatedNode.getFieldNames())));

        result = calculatePercentile.innerJoin(new FieldList(context.modelGuidFieldName, context.percentileFieldName),
                aggregatedNode, new FieldList(context.modelGuidFieldName, context.percentileFieldName));

        List<String> retainColumns = new ArrayList<String>(calculatePercentile.getFieldNames());
        retainColumns.add(context.outputExpRevFieldName);
        result = result.retain(retainColumns.toArray(new String[retainColumns.size()]));
        log.info(String.format("result fields = %s", JsonUtils.serialize(result.getFieldNames())));
        return result;
    }

    private Node lookupPercentileForAverageEV(ParsedContext context, Node node) {
        // Use "ev" field from evScoreDerivation.json to lookup for
        // temporary EV percentile score for average expected revenue
        // value
        if (MapUtils.isNotEmpty(context.originalScoreFieldMap)) {

            FieldList retainedFields = new FieldList(node.getFieldNames());

            node = lookupPercentileFromScoreDerivation(context.scoreDerivationMaps, context.originalScoreFieldMap,
                    context.modelGuidFieldName, context.outputPercentileFieldName, context.expectedRevenueField,
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

        Map<String, Node> nodes = nodeSplitter.split(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            if (scoreDerivationMap.containsKey(modelGuid)) {
                node = node.apply(
                        new LookupPercentileForRevenueFunction( //
                                new Fields(node.getFieldNamesArray()), revenueFieldName, percentileFieldName,
                                scoreDerivationMap.get(modelGuid).get(ScoreDerivationType.EV)),
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
