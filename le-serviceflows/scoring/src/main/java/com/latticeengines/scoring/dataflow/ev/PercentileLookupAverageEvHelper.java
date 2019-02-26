package com.latticeengines.scoring.dataflow.ev;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.LookupPercentileForRevenueFunction;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters.ScoreDerivationType;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;

import cascading.tuple.Fields;

// TODO - rename this class name as we do not calculate average anymore
@Component("percentileLookupAverageEvHelper")
public class PercentileLookupAverageEvHelper {

    @Inject
    public Configuration yarnConfiguration;

    @Inject
    public ModelSummaryProxy modelSummaryProxy;

    @Inject
    private NodeSplitter nodeSplitter;

    public Node calculate(ParsedContext context, Node calculatePercentile) {
        loadScoreDerivations(context);
        return lookupPercentileForAverageEV(context, calculatePercentile);
    }

    private void loadScoreDerivations(ParsedContext context) {
        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps = context.scoreDerivationMaps;
        if (MapUtils.isEmpty(context.scoreDerivationMaps)) {
            scoreDerivationMaps = ExpectedRevenueDataFlowUtil.getScoreDerivationMap(context.customerSpace,
                    yarnConfiguration, modelSummaryProxy, context.originalScoreFieldMap, context.expectedRevenueField,
                    true);
            context.scoreDerivationMaps = scoreDerivationMaps;
        }
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
