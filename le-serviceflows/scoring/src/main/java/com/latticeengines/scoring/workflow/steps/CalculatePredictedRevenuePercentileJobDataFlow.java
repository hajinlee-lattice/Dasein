package com.latticeengines.scoring.workflow.steps;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculatePredictedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CalculatePredictedRevenuePercentileDataFlowConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.spark.exposed.job.score.CalculatePredictedRevenuePercentileJob;

@Component("calculatePredictedRevenuePercentileJobDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalculatePredictedRevenuePercentileJobDataFlow extends
        AbstractCalculateRevenuePercentileJobDataFlow<CalculatePredictedRevenuePercentileDataFlowConfiguration, CalculatePredictedRevenuePercentileJobConfig> {
    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Override
    String getRevenueFieldName() {
        return ScoreResultField.PredictedRevenue.displayName;
    }

    @Override
    String getTargetTableName() {
        return "calculatePredictedRevenuePercentile";
    }

    @Override
    boolean shouldLoadNormalizationRatio() {
        return false;
    }

    @Override
    protected Class<CalculatePredictedRevenuePercentileJob> getJobClz() {
        return CalculatePredictedRevenuePercentileJob.class;
    }

    @Override
    CalculatePredictedRevenuePercentileJobConfig initAndSetDataFlowConfig(String inputTableName, String modelGuidField,
            int percentileLowerBound, int percentileUpperBound, Map<String, String> originalScoreFieldMap,
            Map<String, Double> normalizationRatioMap) {

        CalculatePredictedRevenuePercentileJobConfig config = new CalculatePredictedRevenuePercentileJobConfig();
        config.inputTableName = inputTableName;
        config.percentileFieldName = ScoreResultField.PredictedRevenuePercentile.displayName;
        config.modelGuidField = modelGuidField;
        config.percentileLowerBound = percentileLowerBound;
        config.percentileUpperBound = percentileUpperBound;
        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {
            config.originalScoreFieldMap = originalScoreFieldMap;
        }
        ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap( //
                configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, originalScoreFieldMap);

        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps = ExpectedRevenueDataFlowUtil
                .getNewScoreDerivationMap(customerSpace, yarnConfiguration, modelSummaryProxy, originalScoreFieldMap,
                        ScoreResultField.PredictedRevenue.displayName, true);
        config.scoreDerivationMaps = scoreDerivationMaps;
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
    }
}
