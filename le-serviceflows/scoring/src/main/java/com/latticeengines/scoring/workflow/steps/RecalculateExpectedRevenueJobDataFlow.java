package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculateExpectedRevenueJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculateExpectedRevenueDataFlowConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunBatchSparkJob;
import com.latticeengines.spark.exposed.job.score.RecalculateExpectedRevenueJob;

@Component("recalculateExpectedRevenueJobDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RecalculateExpectedRevenueJobDataFlow
        extends RunBatchSparkJob<RecalculateExpectedRevenueDataFlowConfiguration, RecalculateExpectedRevenueJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(RecalculateExpectedRevenueJobDataFlow.class);

    @Inject
    private Configuration yarnConfiguration;

    @Value("${cdl.scoring.batch.model.size:20}")
    private int batchSize;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    private Table inputTable;

    @Override
    protected Class<RecalculateExpectedRevenueJob> getJobClz() {
        return RecalculateExpectedRevenueJob.class;
    }

    @Override
    protected void postJobExecutions(List<SparkJobResult> results) {

        SparkJobResult mergeResult = CalculateScoreUtils.mergeResult(results, getRandomWorkspace(), yarnConfiguration);
        String customer = configuration.getCustomerSpace().toString();
        String targetTableName = NamingUtils.uuid("RecalculateExpectedRevenue");
        Table targetTable = toTable(targetTableName, mergeResult.getTargets().get(0));
        overlayMetadata(targetTable);
        metadataProxy.createTable(customer, targetTableName, targetTable);
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, targetTableName);
    }


    @Override
    protected List<RecalculateExpectedRevenueJobConfig> batchConfigs(
            RecalculateExpectedRevenueDataFlowConfiguration stepConfiguration) {

        List<RecalculateExpectedRevenueJobConfig> configs = new ArrayList<>();

        String inputTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        inputTable = metadataProxy.getTable(customerSpace.toString(), inputTableName);
        Map<String, String> allScoreFieldMap = ExpectedRevenueDataFlowUtil
                .getScoreFieldsMap(getListObjectFromContext(RATING_MODELS, RatingModelContainer.class));
        String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
        List<Map<String, String>> scoreFieldMaps = CalculateScoreUtils.batchScoreFieldMaps(allScoreFieldMap, modelGuid,
                ScoreResultField.ExpectedRevenue.displayName, batchSize);

        for (int i = 0; i < scoreFieldMaps.size(); i++) {
            Map<String, String> scoreFieldMap = scoreFieldMaps.get(i);
            RecalculateExpectedRevenueJobConfig jobConfig = new RecalculateExpectedRevenueJobConfig();
            jobConfig.inputTableName = inputTableName;
            jobConfig.percentileFieldName = ScoreResultField.Percentile.displayName;
            jobConfig.predictedRevenuePercentileFieldName = ScoreResultField.PredictedRevenuePercentile.displayName;
            jobConfig.expectedRevenueFieldName = ScoreResultField.ExpectedRevenue.displayName;
            jobConfig.modelGuidField = ScoreResultField.ModelId.displayName;
            jobConfig.originalScoreFieldMap = scoreFieldMap;
            jobConfig.fitFunctionParametersMap = //
                    ExpectedRevenueDataFlowUtil.getEVFitFunctionParametersMap( //
                            configuration.getCustomerSpace(), yarnConfiguration, modelSummaryProxy, scoreFieldMap);
            List<DataUnit> inputUnits = new ArrayList<>();
            inputUnits.add(inputTable.toHdfsDataUnit("calculateExpectedRevenue" + i));
            jobConfig.setInput(inputUnits);
            configs.add(jobConfig);
        }
        return configs;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
    }

    private void overlayMetadata(Table targetTable) {
        Map<String, Attribute> attributeMap = new HashMap<>();
        inputTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        super.overlayTableSchema(targetTable, attributeMap);
    }

}
