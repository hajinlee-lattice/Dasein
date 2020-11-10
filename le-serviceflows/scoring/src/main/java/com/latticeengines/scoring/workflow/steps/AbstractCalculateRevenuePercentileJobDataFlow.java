package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.BaseScoringDataFlowStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunBatchSparkJob;

public abstract class AbstractCalculateRevenuePercentileJobDataFlow<T extends BaseScoringDataFlowStepConfiguration, E extends SparkJobConfig>
        extends RunBatchSparkJob<T, E> {

    private static final Logger log = LoggerFactory.getLogger(AbstractCalculateRevenuePercentileJobDataFlow.class);
    private static final String modelGuidField = ScoreResultField.ModelId.displayName;
    private static final int percentileLowerBound = 5;
    private static final int percentileUpperBound = 99;

    @Value("${cdl.scoring.batch.model.size:20}")
    private int batchSize;
    private List<RatingModelContainer> containers;

    private Table inputTable;

    @Inject
    public ModelSummaryProxy modelSummaryProxy;

    abstract String getRevenueFieldName();

    abstract boolean shouldLoadNormalizationRatio();

    abstract String getTargetTableName();

    abstract E initAndSetDataFlowConfig(String inputTableName, String modelGuidField, int percentileLowerBound,
            int percentileUpperBound, Map<String, String> originalScoreFieldMap,
            Map<String, Double> normalizationRatioMap);

    @Override
    protected void postJobExecutions(List<SparkJobResult> results) {
        SparkJobResult mergeResult = CalculateScoreUtils.mergeResult(results, getRandomWorkspace(), yarnConfiguration);
        String customer = configuration.getCustomerSpace().toString();
        String targetTableName = NamingUtils.uuid(getTargetTableName());
        Table targetTable = toTable(targetTableName, mergeResult.getTargets().get(0));
        overlayMetadata(targetTable);
        metadataProxy.createTable(customer, targetTableName, targetTable);
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, targetTableName);

        for (SparkJobResult result : results) {
            postJobExecution(result);
        }
    }

    private void overlayMetadata(Table targetTable) {
        Map<String, Attribute> attributeMap = new HashMap<>();
        inputTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        super.overlayTableSchema(targetTable, attributeMap);
    }

    @Override
    protected List<E> batchConfigs(T stepConfiguration) {
        List<E> configs = new ArrayList<>();
        String inputTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        List<Map<String, String>> scoreFieldMaps = getScoreFieldsMap();
        Map<String, Double> normalizationRatioMaps = shouldLoadNormalizationRatio() ? getNormalizationRatioMap() : null;
        inputTable = metadataProxy.getTable(customerSpace.toString(), inputTableName);

        for (int i = 0; i < scoreFieldMaps.size(); i++) {
            E config = initAndSetDataFlowConfig(inputTableName, modelGuidField, percentileLowerBound,
                    percentileUpperBound, scoreFieldMaps.get(i), normalizationRatioMaps);
            List<DataUnit> inputUnits = new ArrayList<>();
            inputUnits.add(inputTable.toHdfsDataUnit("calculateEVPercentile" + i));
            config.setInput(inputUnits);
            configs.add(config);
        }
        return configs;
    }

    private List<Map<String, String>> getScoreFieldsMap() {
        List<Map<String, String>> resultMaps = new ArrayList<>();
        Map<String, String> originalScoreFieldsMap = new HashMap<>();
        containers = getModelContainers();
        for (RatingModelContainer container : containers) {
            AIModel aiModel = (AIModel) container.getModel();
            String modelGuid = aiModel.getModelSummaryId();
            String scoreField = InterfaceName.RawScore.name();
            if (PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType())) {
                scoreField = getRevenueFieldName();
            }
            originalScoreFieldsMap.put(modelGuid, scoreField);
            if (originalScoreFieldsMap.size() % batchSize == 0) {
                resultMaps.add(originalScoreFieldsMap);
                originalScoreFieldsMap = new HashMap<>();
            }
        }
        if (originalScoreFieldsMap.size() > 0) {
            resultMaps.add(originalScoreFieldsMap);
        }
        if (CollectionUtils.isEmpty(resultMaps)) {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            String modelType = getStringValueFromContext(SCORING_MODEL_TYPE);
            log.info(String.format("modelGuid = %s, modelType = %s", modelGuid, modelType));
            originalScoreFieldsMap.put(modelGuid, getRevenueFieldName());
            resultMaps.add(originalScoreFieldsMap);
        }
        return resultMaps;
    }

    private Map<String, Double> getNormalizationRatioMap() {
        Map<String, Double> normalizationRatioMap = new HashMap<>();
        List<RatingModelContainer> containers = this.containers;
        String tenantId = configuration.getCustomerSpace().toString();
        if (CollectionUtils.isNotEmpty(containers)) {
            for (RatingModelContainer container : containers) {
                AIModel aiModel = (AIModel) container.getModel();
                String modelGuid = aiModel.getModelSummaryId();
                if (PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType())) {
                    ModelSummary modelSummary = modelSummaryProxy.getModelSummary(tenantId, modelGuid);
                    if (modelSummary.getNormalizationRatio() != null) {
                        normalizationRatioMap.put(modelGuid, modelSummary.getNormalizationRatio());
                    }
                }
            }
        } else {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            log.info(String.format("modelGuid = %s", modelGuid));
            ModelSummary modelSummary = modelSummaryProxy.getModelSummary(tenantId, modelGuid);
            if (modelSummary.getNormalizationRatio() != null) {
                normalizationRatioMap.put(modelGuid, modelSummary.getNormalizationRatio());
            }
        }
        return normalizationRatioMap;
    }

    private List<RatingModelContainer> getModelContainers() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        if (allContainers == null) {
            return Collections.emptyList();
        }
        return allContainers.stream() //
                .filter(container -> {
                    RatingEngineType ratingEngineType = container.getEngineSummary().getType();
                    return RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                            || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType);
                }).collect(Collectors.toList());
    }
}
