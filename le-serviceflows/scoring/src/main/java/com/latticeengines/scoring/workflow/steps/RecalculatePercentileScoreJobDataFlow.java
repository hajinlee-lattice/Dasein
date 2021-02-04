package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculatePercentileScoreJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculatePercentileScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunBatchSparkJob;
import com.latticeengines.spark.exposed.job.score.RecalculatePercentileScoreJob;

@Component("recalculatePercentileScoreJobDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RecalculatePercentileScoreJobDataFlow
        extends RunBatchSparkJob<RecalculatePercentileScoreDataFlowConfiguration, RecalculatePercentileScoreJobConfig> {

    private static final String rawScoreField = ScoreResultField.RawScore.displayName;

    private static final String scoreField = ScoreResultField.Percentile.displayName;

    private static final String modelGuidField = ScoreResultField.ModelId.displayName;

    private static final int percentileLowerBound = 5;

    private static final int percentileUpperBound = 99;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Value("${cdl.spark.driver.maxResultSize}")
    private String sparkMaxResultSize;

    @Value("${cdl.scoring.batch.model.size:20}")
    private int batchSize;

    private Table inputTable;

    @Override
    protected Class<RecalculatePercentileScoreJob> getJobClz() {
        return RecalculatePercentileScoreJob.class;
    }

    @Override
    protected List<RecalculatePercentileScoreJobConfig> batchConfigs(
            RecalculatePercentileScoreDataFlowConfiguration stepConfiguration) {

        List<RecalculatePercentileScoreJobConfig> configs = new ArrayList<>();
        Map<String, String> allScoreFieldMap = ExpectedRevenueDataFlowUtil
                .getScoreFieldsMap(getListObjectFromContext(RATING_MODELS, RatingModelContainer.class));
        String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
        List<Map<String, String>> scoreFieldMaps = CalculateScoreUtils.batchScoreFieldMaps(allScoreFieldMap, modelGuid,
                ScoreResultField.RawScore.displayName, batchSize);

        String inputTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        inputTable = metadataProxy.getTable(customerSpace.toString(), inputTableName);

        for (int i = 0; i < scoreFieldMaps.size(); i++) {
            Map<String, String> scoreFieldMap = scoreFieldMaps.get(i);

            RecalculatePercentileScoreJobConfig jobConfig = new RecalculatePercentileScoreJobConfig();
            jobConfig.inputTableName = inputTableName;
            jobConfig.rawScoreFieldName = rawScoreField;
            jobConfig.scoreFieldName = scoreField;
            jobConfig.modelGuidField = modelGuidField;
            jobConfig.percentileLowerBound = percentileLowerBound;
            jobConfig.percentileUpperBound = percentileUpperBound;
            jobConfig.targetScoreDerivation = configuration.isTargetScoreDerivation();
            jobConfig.originalScoreFieldMap = scoreFieldMap;
            jobConfig.targetScoreDerivationInputs = //
                    ExpectedRevenueDataFlowUtil.getTargetScoreFiDerivationInputs( //
                            configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, scoreFieldMap);
            Map<String, String> targetScoreDerivationPaths = ExpectedRevenueDataFlowUtil
                    .getTargetScoreFiDerivationPaths(configuration.getCustomerSpace(), yarnConfiguration,
                            modelSummaryProxy, scoreFieldMap);
            jobConfig.targetScoreDerivationPaths = targetScoreDerivationPaths;

            List<DataUnit> inputUnits = new ArrayList<>();
            inputUnits.add(inputTable.toHdfsDataUnit("calculatePercentileScore" + i));
            jobConfig.setInput(inputUnits);
            configs.add(jobConfig);
        }

        setSparkMaxResultSize(sparkMaxResultSize);
        return configs;

    }

    @Override
    protected void postJobExecutions(List<SparkJobResult> results) {
        SparkJobResult mergeResult = CalculateScoreUtils.mergeResult(results, getRandomWorkspace(), yarnConfiguration);

        String customer = configuration.getCustomerSpace().toString();
        String targetTableName = NamingUtils.uuid("RecalculatePercentileScore");
        Table targetTable = toTable(targetTableName, mergeResult.getTargets().get(0));
        overlayMetadata(targetTable);
        metadataProxy.createTable(customer, targetTableName, targetTable);
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, targetTableName);
        for (SparkJobResult result : results) {
            postJobExecution(result);
        }
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        CalculateScoreUtils.writeTargetScoreDerivations(result, configuration.getCustomerSpace(), yarnConfiguration,
                modelSummaryProxy);
    }

    private void overlayMetadata(Table targetTable) {
        Map<String, Attribute> attributeMap = new HashMap<>();
        inputTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        super.overlayTableSchema(targetTable, attributeMap);
    }
}
