package com.latticeengines.scoring.workflow.steps;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ORIGINAL_BUCKET_METADATA;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scoring.workflow.util.ScoreArtifactRetriever;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.score.PivotScoreAndEventJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("pivotScoreAndEventDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PivotScoreAndEventDataFlow
        extends RunSparkJob<PivotScoreAndEventConfiguration, PivotScoreAndEventJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(PivotScoreAndEventDataFlow.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Value("${cdl.spark.driver.maxResultSize:4g}")
    private String sparkMaxResultSize;

    private boolean multiModel = false;
    private Map<String, List<BucketMetadata>> modelGuidToBucketMetadataMap;
    private Map<String, String> modelGuidToEngineIdMap;
    private Map<String, Boolean> modelGuidToIsEVFlagMap;
    private Map<String, Map<String, Double>> originalLiftMap;

    private CustomerSpace customerSpace;
    private String targetTableName;

    @Override
    protected Class<PivotScoreAndEventJob> getJobClz() {
        return PivotScoreAndEventJob.class;
    }

    @Override
    protected PivotScoreAndEventJobConfig configureJob(PivotScoreAndEventConfiguration stepConfiguration) {

        PivotScoreAndEventJobConfig jobConfig = new PivotScoreAndEventJobConfig();
        String scoreTableName = getStringValueFromContext(PIVOT_SCORE_INPUT_TABLE_NAME);
        targetTableName = scoreTableName + "_pivot";
        customerSpace = CustomerSpace.parse(configuration.getCustomer());

        multiModel = isMultiModel();
        modelGuidToBucketMetadataMap = getModelGuidToBucketMetadataMap();
        modelGuidToEngineIdMap = getModelGuidToEngineIdMap();
        modelGuidToIsEVFlagMap = getModelGuidToIsEVFlagMap();

        Map<String, Double> avgScores = getMapObjectFromContext(SCORING_AVG_SCORES, String.class, Double.class);
        if (MapUtils.isNotEmpty(avgScores)) {
            jobConfig.avgScores = avgScores;
        } else {
            jobConfig.avgScores = ImmutableMap
                    .of(getStringValueFromContext(SCORING_MODEL_ID),
                            getDoubleValueFromContext(SCORING_AVG_SCORE) //
            );
        }
        Map<String, String> scoreFieldMap = getScoreFieldsMap();
        if (MapUtils.isNotEmpty(scoreFieldMap)) {
            jobConfig.scoreFieldMap = scoreFieldMap;
        } else {
            throw new RuntimeException("Cannot determine score fields.");
        }

        // get score derivation and fit function params for model
        jobConfig.scoreDerivationMap = getScoreDerivationMap(jobConfig.scoreFieldMap.keySet(), scoreFieldMap);
        jobConfig.fitFunctionParametersMap = getFitFunctionParametersMap(jobConfig.scoreFieldMap.keySet(),
                scoreFieldMap);

        Table scoreResultTable = metadataProxy.getTable(customerSpace.toString(), scoreTableName);
        List<DataUnit> inputUnits = new ArrayList<>();
        HdfsDataUnit hdfsDataUnit = scoreResultTable.toHdfsDataUnit("scoreResultTable");
        hdfsDataUnit.setCoalesce(true);
        inputUnits.add(hdfsDataUnit);
        jobConfig.setInput(inputUnits);

        setSparkMaxResultSize(sparkMaxResultSize);

        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String customer = configuration.getCustomer();
        Table targetTable = toTable(targetTableName, result.getTargets().get(0));
        metadataProxy.createTable(customer, targetTableName, targetTable);
        putObjectInContext(EVENT_TABLE, targetTable);

        originalLiftMap = getOriginalLifts();
        String targetExtractPath = targetTable.getExtracts().get(0).getPath();
        if (!targetExtractPath.endsWith(".avro")) {
            targetExtractPath = targetExtractPath.endsWith("/") ? targetExtractPath : targetExtractPath + "/";
            targetExtractPath += "*.avro";
        }
        saveBucketedScoreSummary(targetExtractPath);
        putOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_AVRO_PATH, targetExtractPath);
        upsertRatingLifts();

        putStringValueInContext(EXPORT_BUCKET_TOOL_TABLE_NAME, targetTableName);
        String scoreOutputPath = getOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);
        String pivotOutputPath = StringUtils.replace(scoreOutputPath, "_scored_", "_pivoted_");
        putStringValueInContext(EXPORT_BUCKET_TOOL_OUTPUT_PATH, pivotOutputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH, pivotOutputPath);
    }

    private Map<String, String> getScoreDerivationMap(Collection<String> modelIds, Map<String, String> scoreFieldMap) {
        ScoreArtifactRetriever scoreArtifactRetriever = new ScoreArtifactRetriever(modelSummaryProxy,
                yarnConfiguration);
        Map<String, String> scoreDerivationMap = new HashMap<>();
        for (String modelId : modelIds) {
            String scoreDerivation = scoreArtifactRetriever.getScoreDerivation(customerSpace, modelId,
                    InterfaceName.ExpectedRevenue.name().equals(scoreFieldMap.get(modelId)));
            if (scoreDerivation != null) {
                scoreDerivationMap.put(modelId, scoreDerivation);
            }
        }
        return scoreDerivationMap;
    }

    private Map<String, String> getFitFunctionParametersMap(Collection<String> modelIds,
            Map<String, String> scoreFieldMap) {
        ScoreArtifactRetriever scoreArtifactRetriever = new ScoreArtifactRetriever(modelSummaryProxy,
                yarnConfiguration);
        Map<String, String> fitFunctionParametersMap = new HashMap<>();
        for (String modelId : modelIds) {
            String fitFunctionParameters = scoreArtifactRetriever.getFitFunctionParameters(customerSpace, modelId,
                    InterfaceName.ExpectedRevenue.name().equals(scoreFieldMap.get(modelId)));
            if (fitFunctionParameters != null) {
                fitFunctionParametersMap.put(modelId, fitFunctionParameters);
            }
        }
        return fitFunctionParametersMap;
    }

    private void saveBucketedScoreSummary(String targetDataPath) {
        @SuppressWarnings("deprecation")
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, targetDataPath);
        Map<String, List<GenericRecord>> pivotedRecordsMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            if (!pivotedRecordsMap.containsKey(modelGuid)) {
                pivotedRecordsMap.put(modelGuid, new ArrayList<>());
            }
            pivotedRecordsMap.get(modelGuid).add(record);
        }

        Map<String, BucketedScoreSummary> bucketedScoreSummaryMap = new HashMap<>();
        pivotedRecordsMap.forEach((modelGuid, pivotedRecords) -> {
            BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                    .generateBucketedScoreSummary(pivotedRecords, modelGuidToIsEVFlagMap.get(modelGuid));
            List<BucketMetadata> bucketMetadata = modelGuidToBucketMetadataMap.get(modelGuid);
            BucketedScoreSummaryUtils.computeLift(bucketedScoreSummary, bucketMetadata,
                    modelGuidToIsEVFlagMap.get(modelGuid));
            if (Boolean.TRUE.equals(configuration.getSaveBucketMetadata())) {
                log.info("Save bucketed score summary for modelGUID=" + modelGuid + " : "
                        + JsonUtils.serialize(bucketedScoreSummary));
                bucketedScoreProxy.createOrUpdateBucketedScoreSummary(customerSpace.toString(), modelGuid,
                        bucketedScoreSummary);
                log.info("Save bucketed metadata for modelGUID=" + modelGuid + " : "
                        + JsonUtils.serialize(bucketMetadata));
                String engineId = modelGuidToEngineIdMap.get(modelGuid);
                saveABCDBuckets(modelGuid, engineId, bucketMetadata);
            } else {
                bucketedScoreSummaryMap.put(modelGuid, bucketedScoreSummary);
            }
        });
        if (!Boolean.TRUE.equals(configuration.getSaveBucketMetadata())) {
            mergeAggregatedMaps(bucketedScoreSummaryMap);
        }
    }

    private void saveABCDBuckets(String modelGuid, String engineId, List<BucketMetadata> bucketMetadata) {
        boolean isRatingEngine = !modelGuid.equals(engineId);
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        String ratingEngineId = isRatingEngine ? engineId : configuration.getRatingEngineId();
        request.setModelGuid(modelGuid);
        request.setRatingEngineId(ratingEngineId);
        // request.setLastModifiedBy(configuration.getUserId());
        request.setBucketMetadataList(bucketMetadata);
        log.info("Save bucket metadata for modelGuid=" + modelGuid + ", ratingEngineId=" + ratingEngineId + ": "
                + JsonUtils.pprint(bucketMetadata));
        if (getConfiguration().isTargetScoreDerivation()) {
            request.setCreateForModel(true);
        }
        bucketedScoreProxy.createABCDBuckets(customerSpace.toString(), request);
    }

    private Map<String, String> getModelGuidToEngineIdMap() {
        Map<String, String> modelGuidToEngineIdMap = new HashMap<>();
        if (multiModel) {
            List<RatingModelContainer> containers = getModelContainers();
            containers.forEach(container -> {
                AIModel aiModel = (AIModel) container.getModel();
                String modelGuid = aiModel.getModelSummaryId();
                String engineId = container.getEngineSummary().getId();
                modelGuidToEngineIdMap.put(modelGuid, engineId);
            });
        } else {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            modelGuidToEngineIdMap.put(modelGuid, modelGuid);
        }
        return modelGuidToEngineIdMap;
    }

    private Map<String, Boolean> getModelGuidToIsEVFlagMap() {
        Map<String, Boolean> modelGuidToIsEVFlagMap = new HashMap<>();
        if (multiModel) {
            List<RatingModelContainer> containers = getModelContainers();
            containers.forEach(container -> {
                AIModel aiModel = (AIModel) container.getModel();
                String modelGuid = aiModel.getModelSummaryId();
                modelGuidToIsEVFlagMap.put(modelGuid,
                        PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType()));
            });
        } else {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            modelGuidToIsEVFlagMap.put(modelGuid, configuration.isEV());
        }
        return modelGuidToIsEVFlagMap;
    }

    private Map<String, List<BucketMetadata>> getModelGuidToBucketMetadataMap() {
        Map<String, List<BucketMetadata>> modelGuidToBucketMetadataMap = new HashMap<>();
        if (multiModel) {
            List<RatingModelContainer> containers = getModelContainers();
            containers.forEach(container -> {
                AIModel aiModel = (AIModel) container.getModel();
                String modelGuid = aiModel.getModelSummaryId();
                List<BucketMetadata> bucketMetadata = container.getScoringBucketMetadata();
                if (CollectionUtils.isEmpty(bucketMetadata)) {
                    throw new IllegalArgumentException("Must provide bucket metadata for model " + modelGuid);
                }
                modelGuidToBucketMetadataMap.put(modelGuid, bucketMetadata);
            });
        } else {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            List<BucketMetadata> bucketMetadata = getListObjectFromContext(SCORING_BUCKET_METADATA,
                    BucketMetadata.class);
            if (CollectionUtils.isEmpty(bucketMetadata)) {
                throw new IllegalArgumentException("Must provide bucket metadata for model " + modelGuid);
            }
            modelGuidToBucketMetadataMap.put(modelGuid, bucketMetadata);
        }
        return modelGuidToBucketMetadataMap;
    }

    private Map<String, String> getScoreFieldsMap() {
        Map<String, String> scoreFieldsMap;
        if (multiModel) {
            scoreFieldsMap = new HashMap<>();
            List<RatingModelContainer> containers = getModelContainers();
            containers.forEach(container -> {
                AIModel aiModel = (AIModel) container.getModel();
                String modelGuid = aiModel.getModelSummaryId();
                String scoreField = InterfaceName.RawScore.name();
                if (PredictionType.EXPECTED_VALUE.equals(aiModel.getPredictionType())) {
                    scoreField = InterfaceName.ExpectedRevenue.name();
                }
                scoreFieldsMap.put(modelGuid, scoreField);
            });
        } else {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            String scoreField = configuration.getScoreField();
            if (StringUtils.isBlank(scoreField)) {
                throw new IllegalArgumentException("Must specify score field for pivot event and score.");
            }
            if (configuration.isEV()) {
                scoreField = InterfaceName.ExpectedRevenue.name();
            }
            scoreFieldsMap = ImmutableMap.of(modelGuid, scoreField);
        }
        return scoreFieldsMap;
    }

    private void upsertRatingLifts() {
        Map<String, Map<String, Double>> liftMap = new HashMap<>();
        @SuppressWarnings("rawtypes")
        Map<String, Map> mapInContext = getMapObjectFromContext(RATING_LIFTS, String.class, Map.class);
        if (MapUtils.isNotEmpty(mapInContext)) {
            mapInContext.forEach((k, v) -> liftMap.put(k, JsonUtils.convertMap(v, String.class, Double.class)));
        }
        // read lift from original bucket metadata (the latest published version before scoring)
        modelGuidToEngineIdMap.forEach((modelGuid, engineId) -> {
            if (originalLiftMap.containsKey(modelGuid)) {
                Map<String, Double> originalLifts = originalLiftMap.get(modelGuid);
                log.info("Overwrite lifts for " + engineId + " to " + JsonUtils.serialize(originalLifts));
                liftMap.put(engineId, originalLifts);
            } else {
                List<BucketMetadata> bucketMetadata = modelGuidToBucketMetadataMap.get(modelGuid);
                liftMap.put(engineId, new HashMap<>());
                bucketMetadata.forEach(bm -> {
                    String rating = bm.getBucketName();
                    double lift = bm.getLift();
                    liftMap.get(engineId).put(rating, lift);
                });
            }
        });
        putObjectInContext(RATING_LIFTS, liftMap);
    }

    private boolean isMultiModel() {
        return CollectionUtils.isNotEmpty(getModelContainers());
    }

    private List<RatingModelContainer> getModelContainers() {
        return getListObjectFromContext(ITERATION_AI_RATING_MODELS, RatingModelContainer.class);
    }

    private void mergeAggregatedMaps(Map<String, BucketedScoreSummary> bucketedScoreSummaryMap) {
        if (MapUtils.isNotEmpty(bucketedScoreSummaryMap)) {
            Map<String, BucketedScoreSummary> bucketedScoreSummaryMapAgg = getMapObjectFromContext(//
                    BUCKETED_SCORE_SUMMARIES_AGG, String.class, BucketedScoreSummary.class);
            if (MapUtils.isEmpty(bucketedScoreSummaryMapAgg)) {
                bucketedScoreSummaryMapAgg = new HashMap<>();
            }
            bucketedScoreSummaryMapAgg.putAll(bucketedScoreSummaryMap);
            putObjectInContext(BUCKETED_SCORE_SUMMARIES_AGG, bucketedScoreSummaryMapAgg);
        }

        if (MapUtils.isNotEmpty(modelGuidToEngineIdMap)) {
            Map<String, String> modelGuidToEngineIdMapAgg = getMapObjectFromContext(//
                    MODEL_GUID_ENGINE_ID_MAP_AGG, String.class, String.class);
            if (MapUtils.isEmpty(modelGuidToEngineIdMapAgg)) {
                modelGuidToEngineIdMapAgg = new HashMap<>();
            }
            modelGuidToEngineIdMapAgg.putAll(modelGuidToEngineIdMap);
            putObjectInContext(MODEL_GUID_ENGINE_ID_MAP_AGG, modelGuidToEngineIdMapAgg);
        }

        if (MapUtils.isNotEmpty(modelGuidToBucketMetadataMap)) {
            Map<String, List<BucketMetadata>> modelGuidToBucketMetadataMapAgg = new HashMap<>();

            // reconstructing aggregated map from existing map
            @SuppressWarnings("rawtypes")
            Map<String, List> map = getMapObjectFromContext(BUCKET_METADATA_MAP_AGG, String.class, List.class);
            if (MapUtils.isNotEmpty(map)) {
                map.forEach((guid, val) -> {
                    List<BucketMetadata> bmList = JsonUtils.convertList(val, BucketMetadata.class);
                    modelGuidToBucketMetadataMapAgg.put(guid, bmList);
                });
            }

            // add new bmList to aggregated map, with modified lifts
            modelGuidToBucketMetadataMap.forEach((guid, bmList) -> {
                if (CollectionUtils.isNotEmpty(bmList)) {
                    Map<String, Double> originalLift = originalLiftMap.get(guid);
                    bmList.forEach(bm -> bm.setLift(originalLift.getOrDefault(bm.getBucketName(), bm.getLift())));
                }
                modelGuidToBucketMetadataMapAgg.put(guid, bmList);
            });

            // TODO: remove this log later
            log.info("BUCKET_METADATA_MAP_AGG is " + JsonUtils.serialize(modelGuidToBucketMetadataMapAgg));
            putObjectInContext(BUCKET_METADATA_MAP_AGG, modelGuidToBucketMetadataMapAgg);
        }
    }


    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Double>> getOriginalLifts() {
        Map<String, Map<String, Double>> liftMap = new HashMap<>();
        Map<String, List<BucketMetadata>> bmMap = WorkflowStaticContext.getObject(ORIGINAL_BUCKET_METADATA, Map.class);
        if (MapUtils.isNotEmpty(bmMap)) {
            bmMap.forEach((guid, bmList) -> {
                Map<String, Double> lifts = new HashMap<>();
                if (CollectionUtils.isNotEmpty(bmList)) {
                    bmList.forEach(bm -> lifts.put(bm.getBucketName(), bm.getLift()));
                }
                liftMap.put(guid, lifts);
            });
        }
        return liftMap;
    }

}
