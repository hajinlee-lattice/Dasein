package com.latticeengines.scoring.workflow.steps;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CalculateExpectedRevenuePercentileDataFlowConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.exposed.job.score.CalculateExpectedRevenuePercentileJob;

@Component("calculateExpectedRevenuePercentileJobDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalculateExpectedRevenuePercentileJobDataFlow extends
        AbstractCalculateRevenuePercentileJobDataFlow<CalculateExpectedRevenuePercentileDataFlowConfiguration, CalculateExpectedRevenuePercentileJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(CalculateExpectedRevenuePercentileJobDataFlow.class);

    @Value("${cdl.spark.driver.maxResultSize}")
    private String sparkMaxResultSize;

    @Override
    String getRevenueFieldName() {
        return ScoreResultField.ExpectedRevenue.displayName;
    }

    @Override
    boolean shouldLoadNormalizationRatio() {
        return true;
    }

    @Override
    String getTargetTableName() {
        return "calculateExpectedRevenuePercentile";
    }

    @Override
    protected Class<CalculateExpectedRevenuePercentileJob> getJobClz() {
        return CalculateExpectedRevenuePercentileJob.class;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        CalculateScoreUtils.writeTargetScoreDerivations(result, configuration.getCustomerSpace(), yarnConfiguration,
                modelSummaryProxy);
    }

    @Override
    CalculateExpectedRevenuePercentileJobConfig initAndSetDataFlowConfig(String inputTableName, String modelGuidField,
            int percentileLowerBound, int percentileUpperBound, Map<String, String> originalScoreFieldMap,
            Map<String, Double> normalizationRatioMap) {
        CalculateExpectedRevenuePercentileJobConfig config = new CalculateExpectedRevenuePercentileJobConfig();
        config.inputTableName = inputTableName;
        config.percentileFieldName = ScoreResultField.ExpectedRevenuePercentile.displayName;
        config.modelGuidField = modelGuidField;
        config.percentileLowerBound = percentileLowerBound;
        config.percentileUpperBound = percentileUpperBound;
        config.normalizationRatioMap = normalizationRatioMap;
        config.targetScoreDerivation = configuration.isTargetScoreDerivation();
        config.targetScoreDerivationInputs = //
                ExpectedRevenueDataFlowUtil.getTargetScoreFiDerivationInputs( //
                        configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, originalScoreFieldMap);
        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {
            config.originalScoreFieldMap = originalScoreFieldMap;
        }
        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps = ExpectedRevenueDataFlowUtil
                .getNewScoreDerivationMap(
                customerSpace, yarnConfiguration, modelSummaryProxy, originalScoreFieldMap,
                ScoreResultField.ExpectedRevenue.displayName, true);
        config.scoreDerivationMaps = scoreDerivationMaps;

        // load evFitFunctionParamaters
        config.fitFunctionParametersMap = ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap(
                configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, originalScoreFieldMap, null);

        log.info(String.format("fitFunctionParametersMap = %s", JsonUtils.serialize(config.fitFunctionParametersMap)));

        setSparkMaxResultSize(sparkMaxResultSize);

        return config;
    }

}
