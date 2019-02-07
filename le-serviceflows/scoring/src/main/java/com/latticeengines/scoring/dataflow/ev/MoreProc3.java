package com.latticeengines.scoring.dataflow.ev;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculateFittedExpectedRevenueFunction;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

import cascading.tuple.Fields;

@Component("moreProc3")
public class MoreProc3 {
    private static final Logger log = LoggerFactory.getLogger(MoreProc3.class);

    @Inject
    private Helper helper;

    public Node additionalProcessing2(ParsedContext context, Node calculatePercentile, FieldList retainedFields) {
        calculatePercentile = calculatePercentile.addColumnWithFixedValue(context.outputExpRevFieldName, null,
                Double.class);
        // 6. load evFitFunctionParamaters
        //
        // 7. initialize expectedRevenueFitter based on corresponding
        // fit function parameters
        //
        // 8. for each row
        // 8.1. calculate fitted expected revenue using this new
        // temporary EV percentile score and set it back to
        // ExpectedRevenue column
        //
        // 9. copy values of ExpectedRevenuePercentile in original
        // percentile column ("Score") as downstream processing expects
        // final percentiles into original percentile column
        calculatePercentile = calculateFittedExpectedRevenue(context.fitFunctionParametersMap,
                context.originalScoreFieldMap, context.modelGuidFieldName, context.outputExpRevFieldName,
                context.outputPercentileFieldName, context.minPct, context.maxPct, calculatePercentile);

        log.info(String.format("Drop standardScoreField '%s'", context.standardScoreField));
        calculatePercentile = calculatePercentile.discard(context.standardScoreField);
        log.info(String.format("Rename temporary ScoreField '%s' to standardScoreField '%s'",
                context.outputPercentileFieldName, context.standardScoreField));

        calculatePercentile = calculatePercentile.rename(new FieldList(context.outputPercentileFieldName),
                new FieldList(context.standardScoreField));

        log.info(String.format("Drop expectedRevenueField '%s'", context.expectedRevenueField));
        calculatePercentile = calculatePercentile.discard(context.expectedRevenueField);
        log.info(String.format("Rename temporary outputExpRevFieldName '%s' to expectedRevenueField '%s'",
                context.outputExpRevFieldName, context.expectedRevenueField));

        calculatePercentile = calculatePercentile.rename(new FieldList(context.outputExpRevFieldName),
                new FieldList(context.expectedRevenueField));

        calculatePercentile = calculatePercentile.retain(retainedFields);
        return calculatePercentile;
    }

    private Node calculateFittedExpectedRevenue(Map<String, String> fitFunctionParametersMap,
            Map<String, String> originalScoreFieldMap, //
            String modelGuidFieldName, String outputExpRevFieldName, String outputPercentileFieldName, int minPct,
            int maxPct, Node mergedScoreCount) {

        Map<String, Node> nodes = helper.splitNodes(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            String evFitFunctionParamsStr = fitFunctionParametersMap.get(modelGuid);

            Node node = entry.getValue();

            if (ScoreResultField.ExpectedRevenue.displayName //
                    .equals(originalScoreFieldMap.get(modelGuid))) {
                // 6. load evFitFunctionParamaters
                // 7. initialize expectedRevenueFitter based on corresponding
                // fit function parameters
                // 8. for each row
                // 8.1. calculate fitted expected revenue using this new
                // temporary EV percentile score and set it back to
                // ExpectedRevenue column
                // 9. copy values of ExpectedRevenuePercentile in original
                // percentile column ("Score") as downstream processing expects
                // final percentiles into original percentile column
                node = node.apply(
                        new CalculateFittedExpectedRevenueFunction(new Fields(node.getFieldNamesArray()),
                                outputExpRevFieldName, outputPercentileFieldName, evFitFunctionParamsStr),
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
