package com.latticeengines.scoring.dataflow;

import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculateFittedExpectedRevenueFunction;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;

import cascading.tuple.Fields;

@Component("calculateExpectedRevenuePercentile")
public class CalculateExpectedRevenuePercentile
        extends AbstractCalculateRevenuePercentile<CalculateExpectedRevenuePercentileParameters> {
    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenuePercentile.class);

    private static final String PREFIX_TEMP_COL = "__TEMP__";
    private static final String EV_PERCENTILE_EXPRESSION = "%s != null ? %s : %s";

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    private String expectedRevenueField = ScoreResultField.ExpectedRevenue.displayName;

    @Override
    protected void parseParamAndSetFields(CalculateExpectedRevenuePercentileParameters parameters) {
        CustomerSpace customerSpace = parameters.getCustomerSpace();
        inputTableName = parameters.getInputTableName();
        percentileFieldName = parameters.getPercentileFieldName();
        modelGuidFieldName = parameters.getModelGuidField();
        originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
        minPct = parameters.getPercentileLowerBound();
        maxPct = parameters.getPercentileUpperBound();

        fitFunctionParametersMap = ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap(customerSpace,
                yarnConfiguration, modelSummaryProxy, originalScoreFieldMap, parameters.getFitFunctionParametersMap());
        log.info(String.format("fitFunctionParametersMap = %s", JsonUtils.serialize(fitFunctionParametersMap)));
    }

    @SuppressWarnings("deprecation")
    protected Node additionalProcessing(Node calculatePercentile) {
        log.info(String.format("percentileFieldName '%s', standardScoreField '%s'", percentileFieldName,
                standardScoreField));
        if (!standardScoreField.equals(percentileFieldName)) {
            FieldList retainedFields = new FieldList(calculatePercentile.getFieldNames());
            long timestamp = System.currentTimeMillis();
            String outputPercentileFieldName = String.format("%sper_%d", PREFIX_TEMP_COL, timestamp);
            log.info(String.format("Using temporary ScoreField '%s' to merge values from %s and %s",
                    outputPercentileFieldName, percentileFieldName, standardScoreField));

            String outputExpRevFieldName = String.format("%sev_%d", PREFIX_TEMP_COL, timestamp);
            log.info(String.format("Using temporary evField '%s' to generate and store final ev using "
                    + "fitfunction and ev percentile field %s", outputExpRevFieldName, percentileFieldName));

            calculatePercentile = calculatePercentile //
                    .addFunction(
                            String.format(EV_PERCENTILE_EXPRESSION, percentileFieldName, percentileFieldName,
                                    standardScoreField), //
                            new FieldList(percentileFieldName, standardScoreField), //
                            new FieldMetadata(outputPercentileFieldName, Integer.class));

            calculatePercentile = calculatePercentile.addColumnWithFixedValue(outputExpRevFieldName, null,
                    Double.class);

            calculatePercentile = calculateFittedExpectedRevenue(originalScoreFieldMap, modelGuidFieldName,
                    outputExpRevFieldName, outputPercentileFieldName, minPct, maxPct, calculatePercentile);

            log.info(String.format("Drop standardScoreField '%s'", standardScoreField));
            calculatePercentile = calculatePercentile.discard(standardScoreField);
            log.info(String.format("Rename temporary ScoreField '%s' to standardScoreField '%s'",
                    outputPercentileFieldName, standardScoreField));

            calculatePercentile = calculatePercentile.rename(new FieldList(outputPercentileFieldName),
                    new FieldList(standardScoreField));

            log.info(String.format("Drop expectedRevenueField '%s'", expectedRevenueField));
            calculatePercentile = calculatePercentile.discard(expectedRevenueField);
            log.info(String.format("Rename temporary outputExpRevFieldName '%s' to expectedRevenueField '%s'",
                    outputExpRevFieldName, expectedRevenueField));

            calculatePercentile = calculatePercentile.rename(new FieldList(outputExpRevFieldName),
                    new FieldList(expectedRevenueField));

            calculatePercentile = calculatePercentile.retain(retainedFields);
        }
        return calculatePercentile;
    }

    private Node calculateFittedExpectedRevenue(Map<String, String> originalScoreFieldMap, //
            String modelGuidFieldName, String outputExpRevFieldName, String outputPercentileFieldName, int minPct,
            int maxPct, Node mergedScoreCount) {

        Map<String, Node> nodes = splitNodes(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            String evFitFunctionParamsStr = fitFunctionParametersMap.get(modelGuid);

            Node node = entry.getValue();

            if (ScoreResultField.ExpectedRevenue.displayName //
                    .equals(originalScoreFieldMap.get(modelGuid))) {
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
