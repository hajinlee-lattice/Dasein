package com.latticeengines.scoring.dataflow;

import java.util.Map;

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
import com.latticeengines.dataflow.runtime.cascading.cdl.LookupPercentileForRevenueFunction;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.scoring.dataflow.ev.Helper;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil;
import com.latticeengines.scoring.workflow.steps.ExpectedRevenueDataFlowUtil.ScoreDerivationType;

import cascading.tuple.Fields;

@Component("calculatePredictedRevenuePercentile")
public class CalculatePredictedRevenuePercentile
        extends TypesafeDataFlowBuilder<CalculatePredictedRevenuePercentileParameters> {
    private static final Logger log = LoggerFactory.getLogger(CalculatePredictedRevenuePercentile.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private Helper helper;

    protected String inputTableName;
    protected String percentileFieldName;
    protected String modelGuidFieldName;
    protected Map<String, String> originalScoreFieldMap;
    protected int minPct = 5;
    protected int maxPct = 99;
    protected String standardScoreField = ScoreResultField.Percentile.displayName;
    protected Map<String, String> fitFunctionParametersMap;

    @Override
    public Node construct(CalculatePredictedRevenuePercentileParameters parameters) {
        log.info(String.format("%s = %s", parameters.getClass().getSimpleName(), JsonUtils.serialize(parameters)));
        parseParamAndSetFields(parameters);

        Node inputTable = addSource(inputTableName);
        Node addPercentileColumn = inputTable.addColumnWithFixedValue(percentileFieldName, null, Integer.class);
        FieldList retainedFields = new FieldList(addPercentileColumn.getFieldNames());

        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {

            Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMap = ExpectedRevenueDataFlowUtil
                    .getScoreDerivationMap(parameters.getCustomerSpace(), yarnConfiguration, modelSummaryProxy,
                            originalScoreFieldMap, true);

            Node calculatePercentile = lookupPercentileFromScoreDerivation(scoreDerivationMap, originalScoreFieldMap,
                    modelGuidFieldName, percentileFieldName, parameters.getRevenueFieldName(), minPct, maxPct,
                    addPercentileColumn);
            calculatePercentile = calculatePercentile.retain(retainedFields);
            return calculatePercentile;
        }
        return addPercentileColumn;
    }

    private Node lookupPercentileFromScoreDerivation(
            Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMap, //
            Map<String, String> originalScoreFieldMap, //
            String modelGuidFieldName, String percentileFieldName, String revenueFieldName, int minPct, int maxPct,
            Node mergedScoreCount) {

        Map<String, Node> nodes = helper.splitNodes(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
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

    private void parseParamAndSetFields(CalculatePredictedRevenuePercentileParameters parameters) {
        inputTableName = parameters.getInputTableName();
        percentileFieldName = parameters.getPercentileFieldName();
        modelGuidFieldName = parameters.getModelGuidField();
        originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
        minPct = parameters.getPercentileLowerBound();
        maxPct = parameters.getPercentileUpperBound();
    }
}
