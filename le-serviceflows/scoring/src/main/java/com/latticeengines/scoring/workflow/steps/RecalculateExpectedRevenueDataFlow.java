package com.latticeengines.scoring.workflow.steps;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculateExpectedRevenueParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculateExpectedRevenueDataFlowConfiguration;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("recalculateExpectedRevenueDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RecalculateExpectedRevenueDataFlow extends RunDataFlow<RecalculateExpectedRevenueDataFlowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RecalculateExpectedRevenueDataFlow.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Value("${cdl.big.dataflow.am.mem.gb}")
    private int amMemGbForPA;

    @Value("${cdl.big.dataflow.am.vcores}")
    private int amVCoresForPA;

    @Override
    public void execute() {
        preDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, configuration.getTargetTableName());
    }

    private void preDataFlow() {
        String inputTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);

        RecalculateExpectedRevenueParameters params = new RecalculateExpectedRevenueParameters();
        params.setInputTableName(inputTableName);
        params.setPercentileFieldName(ScoreResultField.Percentile.displayName);
        params.setPredictedRevenuePercentileFieldName(ScoreResultField.PredictedRevenuePercentile.displayName);
        params.setExpectedRevenueFieldName(ScoreResultField.ExpectedRevenue.displayName);
        params.setModelGuidField(ScoreResultField.ModelId.displayName);

        Map<String, String> scoreFieldMap = ExpectedRevenueDataFlowUtil
                .getScoreFieldsMap(getListObjectFromContext(RATING_MODELS, RatingModelContainer.class));
        String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);

        if (MapUtils.isNotEmpty(scoreFieldMap)) {
            log.info(String.format("Using scoreFieldMap %s to set FitFunctionParametersMap",
                    JsonUtils.serialize(scoreFieldMap)));
            params.setOriginalScoreFieldMap(scoreFieldMap);
        } else if (StringUtils.isNotBlank(modelGuid)) {
            log.info(String.format("Using individual modelGuid %s to set FitFunctionParametersMap", modelGuid));
            scoreFieldMap = new HashMap<>();
            scoreFieldMap.put(modelGuid, ScoreResultField.ExpectedRevenue.displayName);
            params.setOriginalScoreFieldMap(scoreFieldMap);
        } else {
            throw new RuntimeException("Couldn't find any valid scoreFieldMap or individual modelGuid");
        }
        params.setFitFunctionParametersMap( //
                ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap( //
                        configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, scoreFieldMap));
        configuration.setDataFlowParams(params);
    }

    private boolean inPA() {
        return getNamespace().contains(ProcessAnalyzeWorkflowConfiguration.WORKFLOW_NAME);
    }

    protected Integer getYarnAmMemGb() {
        if (inPA()) {
            return amMemGbForPA;
        } else {
            return super.getYarnAmMemGb();
        }
    }

    protected Integer getYarnAmVCores() {
        if (inPA()) {
            return amVCoresForPA;
        } else {
            return super.getYarnAmVCores();
        }
    }

}
