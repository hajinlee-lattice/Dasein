package com.latticeengines.scoring.workflow.steps;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculateExpectedRevenueParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculateExpectedRevenueDataFlowConfiguration;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("recalculateExpectedRevenueDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RecalculateExpectedRevenueDataFlow extends RunDataFlow<RecalculateExpectedRevenueDataFlowConfiguration> {
    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

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
        if (MapUtils.isNotEmpty(scoreFieldMap)) {
            params.setOriginalScoreFieldMap(scoreFieldMap);
            params.setFitFunctionParametersMap( //
                    ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap( //
                            configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, scoreFieldMap));
        }
        configuration.setDataFlowParams(params);
    }
}
