package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CombineInputTableWithScoreJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.score.CombineInputTableWithScoreJob;

@Component("combineInputTableWithScoreDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CombineInputTableWithScoreDataFlow
        extends RunSparkJob<CombineInputTableWithScoreDataFlowConfiguration, CombineInputTableWithScoreJobConfig> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CombineInputTableWithScoreDataFlow.class);

    private Table scoreResultTable;
    private Table inputTable;

    @Override
    protected Class<CombineInputTableWithScoreJob> getJobClz() {
        return CombineInputTableWithScoreJob.class;
    }

    @Override
    protected CombineInputTableWithScoreJobConfig configureJob(
            CombineInputTableWithScoreDataFlowConfiguration stepConfiguration) {

        CombineInputTableWithScoreJobConfig jobConfig = new CombineInputTableWithScoreJobConfig();
        jobConfig.bucketMetadata = getBucketMetadata();
        jobConfig.modelType = getModelType();
        jobConfig.idColumn = configuration.getIdColumnName();
        if (configuration.isCdlMultiModel()) {
            setCdlMultiModelParams(jobConfig);
        }

        scoreResultTable = metadataProxy.getTable(customerSpace.toString(), getScoreResultTableName());
        List<DataUnit> inputUnits = new ArrayList<>();
        inputTable = metadataProxy.getTable(customerSpace.toString(), getInputTableName());
        inputUnits.add(inputTable.toHdfsDataUnit("inputResult"));
        inputUnits.add(scoreResultTable.toHdfsDataUnit("scoreResult"));
        jobConfig.setInput(inputUnits);
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String customer = configuration.getCustomer();
        String targetTableName = NamingUtils.uuid("CombineInputTableWithScore");
        Table targetTable = toTable(targetTableName, result.getTargets().get(0));
        overlayMetadata(targetTable);
        metadataProxy.createTable(customer, targetTableName, targetTable);

        putStringValueInContext(EXPORT_SCORE_TRAINING_FILE_TABLE_NAME, targetTableName);
        putStringValueInContext(PIVOT_SCORE_INPUT_TABLE_NAME, targetTableName);
        putStringValueInContext(AI_RAW_RATING_TABLE_NAME, targetTableName);
    }

    private void overlayMetadata(Table targetTable) {
        Map<String, Attribute> attributeMap = new HashMap<>();
        inputTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        scoreResultTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        super.overlayTableSchema(targetTable, attributeMap);
    }

    private void setCdlMultiModelParams(CombineInputTableWithScoreJobConfig jobConfig) {
        if (!configuration.isCdlMultiModel())
            return;

        List<RatingModelContainer> containers = getModelContainers();
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no AI models in context.");
        }
        Map<String, List<BucketMetadata>> bucketMetadataMap = new HashMap<>();

        containers.forEach(container -> {
            List<BucketMetadata> bucketMetadata = getSingleModelBucketMetadata(container);
            String modelGuid = ((AIModel) container.getModel()).getModelSummaryId();
            bucketMetadata.sort(Comparator.comparingInt(BucketMetadata::getRightBoundScore));
            bucketMetadataMap.put(modelGuid, bucketMetadata);
        });

        jobConfig.bucketMetadataMap = bucketMetadataMap;
        jobConfig.scoreFieldName = InterfaceName.Score.name();
        jobConfig.idColumn = InterfaceName.__Composite_Key__.toString();
        jobConfig.modelIdField = ScoreResultField.ModelId.displayName;
    }

    private List<BucketMetadata> getSingleModelBucketMetadata(RatingModelContainer container) {
        AIModel aiModel = (AIModel) container.getModel();
        List<BucketMetadata> bucketMetadata = container.getScoringBucketMetadata();
        if (CollectionUtils.isEmpty(bucketMetadata)) {
            throw new IllegalArgumentException("AI model " + aiModel.getId() + " does not have bucket metadata.");
        }
        return bucketMetadata;
    }

    private List<RatingModelContainer> getModelContainers() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(ITERATION_RATING_MODELS,
                RatingModelContainer.class);
        return allContainers.stream() //
                .filter(container -> {
                    RatingEngineType ratingEngineType = container.getEngineSummary().getType();
                    return RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                            || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType);
                }) //
                .collect(Collectors.toList());
    }

    private String getInputTableName() {
        if (configuration.isCdlMultiModel() && !configuration.isExportKeyColumnsOnly()) {
            Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
            if (eventTable != null) {
                log.info("Use event table=" + eventTable.getName());
                return eventTable.getName();
            }
        }
        String inputTableName = getStringValueFromContext(FILTER_EVENT_TARGET_TABLE_NAME);
        if (StringUtils.isBlank(inputTableName)) {
            inputTableName = getConfiguration().getInputTableName();
        }
        return inputTableName;
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTableName == null) {
            scoreResultTableName = getConfiguration().getScoreResultsTableName();
        }
        return scoreResultTableName;
    }

    private List<BucketMetadata> getBucketMetadata() {
        List<BucketMetadata> bucketMetadata = configuration.getBucketMetadata();
        if (CollectionUtils.isEmpty(bucketMetadata)) {
            bucketMetadata = BucketMetadataUtils.getDefaultMetadata();
        }
        putObjectInContext(SCORING_BUCKET_METADATA, bucketMetadata);
        bucketMetadata.sort(Comparator.comparingInt(BucketMetadata::getRightBoundScore));
        return bucketMetadata;
    }

    private String getModelType() {
        return configuration.getModelType();
    }
}
