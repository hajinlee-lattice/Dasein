package com.latticeengines.scoring.dataflow.ev;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.atlas.CalculateFittedExpectedRevenueFunction;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters.ScoreDerivationType;
import com.latticeengines.scoring.dataflow.CalculateExpectedRevenuePercentile.ParsedContext;

import cascading.tuple.Fields;

@Component("calculateFittedExpectedRevenueHelper")
public class CalculateFittedExpectedRevenueHelper {

    @Inject
    private NodeSplitter nodeSplitter;

    public Node calculate(ParsedContext context, Node calculatePercentile, FieldList retainedFields) {
        Map<String, Node> nodes = nodeSplitter.split(calculatePercentile, context.originalScoreFieldMap,
                context.modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            String evFitFunctionParamsStr = context.fitFunctionParametersMap.get(modelGuid);
            Double normalizationRatio = context.normalizationRatioMap.get(modelGuid);
            Double avgProbabilityTestDataset = getAvgProbabilityTestDataset(context, modelGuid);

            Node node = entry.getValue();

            if (ScoreResultField.ExpectedRevenue.displayName //
                    .equals(context.originalScoreFieldMap.get(modelGuid))) {
                node = node.apply(
                        new CalculateFittedExpectedRevenueFunction(new Fields(node.getFieldNamesArray()),
                                context.expectedRevenueField, //
                                context.outputPercentileFieldName, //
                                context.probabilityField, //
                                context.backupProbabilityFieldName, //
                                context.predictedRevenueField, //
                                context.backupPredictedRevFieldName, //
                                normalizationRatio, //
                                avgProbabilityTestDataset, //
                                evFitFunctionParamsStr),
                        new FieldList(node.getFieldNamesArray()), node.getSchema(), null, Fields.REPLACE);
            }

            if (merged == null) {
                merged = node;
            } else {
                merged = merged.merge(node);
            }
        }

        return merged.discard(context.standardScoreField)//
                .rename(new FieldList(context.outputPercentileFieldName), //
                        new FieldList(context.standardScoreField)) //
                .retain(retainedFields);
    }

    private Double getAvgProbabilityTestDataset(ParsedContext context, String modelGuid) {
        Double avgProbabilityTestDataset = null;
        if (MapUtils.isNotEmpty(context.scoreDerivationMaps)
                && MapUtils.isNotEmpty(context.scoreDerivationMaps.get(modelGuid))
                && context.scoreDerivationMaps.get(modelGuid).get(ScoreDerivationType.PROBABILITY) != null) {
            avgProbabilityTestDataset = context.scoreDerivationMaps.get(modelGuid)
                    .get(ScoreDerivationType.PROBABILITY).averageProbability;
        }
        return avgProbabilityTestDataset;
    }
}
