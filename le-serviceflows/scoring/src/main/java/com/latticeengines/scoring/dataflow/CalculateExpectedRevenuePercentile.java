package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculateFittedExpectedRevenueFunction;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePercentile;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;

import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

@Component("calculateExpectedRevenuePercentile")
public class CalculateExpectedRevenuePercentile
        extends TypesafeDataFlowBuilder<CalculateExpectedRevenuePercentileParameters> {
    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenuePercentile.class);

    private static final String PREFIX_TEMP_COL = "__TEMP__";
    private static final String EV_PERCENTILE_EXPRESSION = "%s != null ? %s : %s";

    private static final String SCORE_COUNT_FIELD_NAME = ScoreResultField.RawScore.displayName + "_count";

    protected String inputTableName;
    protected String percentileFieldName;
    protected String modelGuidFieldName;
    protected Map<String, String> originalScoreFieldMap;
    protected int minPct = 5;
    protected int maxPct = 99;
    protected String standardScoreField = ScoreResultField.Percentile.displayName;
    protected Map<String, String> fitFunctionParametersMap;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    private String expectedRevenueField = ScoreResultField.ExpectedRevenue.displayName;

    @Override
    public Node construct(CalculateExpectedRevenuePercentileParameters parameters) {
        log.info(String.format("%s = %s", parameters.getClass().getSimpleName(), JsonUtils.serialize(parameters)));
        parseParamAndSetFields(parameters);

        Node inputTable = addSource(inputTableName);
        Node addPercentileColumn = inputTable.addColumnWithFixedValue(percentileFieldName, null, Integer.class);
        FieldList retainedFields = new FieldList(addPercentileColumn.getFieldNames());

        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {
            Node mergedScoreCount = mergeCount(addPercentileColumn, modelGuidFieldName);
            Node calculatePercentile = calculatePercentileByFieldMap(originalScoreFieldMap, modelGuidFieldName,
                    percentileFieldName, minPct, maxPct, mergedScoreCount);
            calculatePercentile = calculatePercentile.retain(retainedFields);
            calculatePercentile = additionalProcessing(calculatePercentile);
            return calculatePercentile;
        }
        return addPercentileColumn;
    }

    private Node calculatePercentileByFieldMap(Map<String, String> originalScoreFieldMap, //
            String modelGuidFieldName, String percentileFieldName, int minPct, int maxPct, Node mergedScoreCount) {

        Map<String, Node> nodes = splitNodes(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();

            String originalScoreField = originalScoreFieldMap.getOrDefault(modelGuid, InterfaceName.RawScore.name());

            Node output = calculatePercentileByFieldName(modelGuidFieldName, originalScoreField, percentileFieldName,
                    minPct, maxPct, node);
            if (merged == null) {
                merged = output;
            } else {
                merged = merged.merge(output);
            }
        }
        return merged;
    }

    protected Map<String, Node> splitNodes(Node input, Map<String, String> originalScoreFieldMap,
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

    private Node calculatePercentileByFieldName(String modelGuidFieldName, String originalScoreFieldName,
            String percentileFieldName, int minPct, int maxPct, Node node) {
        if (ScoreResultField.RawScore.displayName.equals(originalScoreFieldName)) {
            return node;
        }

        node = node.addColumnWithFixedValue(percentileFieldName, null, Integer.class);
        List<String> returnedFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> returnedMetadata = new ArrayList<>(node.getSchema());
        Node calculatePercentile = node
                .groupByAndBuffer(new FieldList(modelGuidFieldName), new FieldList(originalScoreFieldName),
                        new CalculatePercentile(new Fields(returnedFields.toArray(new String[0])), minPct, maxPct,
                                percentileFieldName, SCORE_COUNT_FIELD_NAME, originalScoreFieldName),
                        true, returnedMetadata);
        return calculatePercentile;
    }

    private Node mergeCount(Node node, String modelGuidFieldName) {
        String scoreCountPipeName = "ModelScoreCount_" + UUID.randomUUID().toString().replace("-", "") + "_";
        String scoreFieldName = ScoreResultField.Percentile.displayName;
        Node score = node.retain(scoreFieldName, modelGuidFieldName).renamePipe(scoreCountPipeName);
        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                new FieldMetadata(modelGuidFieldName, String.class), //
                new FieldMetadata(scoreFieldName, String.class), //
                new FieldMetadata(SCORE_COUNT_FIELD_NAME, Long.class) //
        );
        Node totalCount = score
                .groupByAndAggregate(new FieldList(modelGuidFieldName), //
                        new Count(new Fields(SCORE_COUNT_FIELD_NAME)), //
                        scoreCountFms, Fields.ALL) //
                .retain(modelGuidFieldName, SCORE_COUNT_FIELD_NAME);
        return node.innerJoin(modelGuidFieldName, totalCount, modelGuidFieldName);
    }

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
