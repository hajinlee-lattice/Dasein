package com.latticeengines.scoring.workflow.steps;

import static com.latticeengines.scoring.dataflow.ComputeLift.RATING_COUNT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.ComputeLiftParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ComputeLiftDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("computeLiftDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ComputeLiftDataFlow extends RunDataFlow<ComputeLiftDataFlowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ComputeLiftDataFlow.class);

    private static final String modelGuidField = ScoreResultField.ModelId.displayName;
    private static final String ratingField = ScoreResultField.Rating.displayName;
    private static final String liftField = InterfaceName.Lift.name();

    @Override
    public void execute() {
        preDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        Map<String, String> scoreFieldMap = getScoreFieldsMap();
        Table liftTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration,
                liftTable.getExtracts().get(0).getPath() + "/*.avro");
        Map<String, Map<String, Double>> liftMap = new HashMap<>();
        Map<String, String> modelGuidToEngineIdMap = getModelGuidToEngineIdMap();
        Map<String, List<BucketMetadata>> modelGuidToBucketMetadataMap = getModelGuidToBucketMetadataMap();
        records.forEach(record -> {
            String modelGuid = record.get(modelGuidField).toString();
            String rating = record.get(ratingField).toString();
            Object liftObj = record.get(liftField);
            final Double lift = liftObj instanceof Double ? (Double) liftObj : 0.0D;
            Long count = (Long) record.get(RATING_COUNT);

            List<BucketMetadata> bucketMetadata = modelGuidToBucketMetadataMap.get(modelGuid);
            bucketMetadata.forEach(metadata -> {
                if (rating.equals(metadata.getBucketName())) {
                    metadata.setLift(lift);
                    metadata.setNumLeads(count.intValue());
                }
            });

            String scoreField = scoreFieldMap.get(modelGuid);
            String engineId = modelGuidToEngineIdMap.get(modelGuid);
            if (InterfaceName.ExpectedRevenue.name().equals(scoreField)) {
                engineId = RatingEngine.toRatingAttrName(engineId, RatingEngine.ScoreType.ExpectedRevenue);
            }
            if (!liftMap.containsKey(engineId)) {
                liftMap.put(engineId, new HashMap<>());
            }
            liftMap.get(engineId).put(rating, lift);
        });
        putObjectInContext(RATING_LIFTS, liftMap);
        modelGuidToEngineIdMap.forEach((modelGuid, engineId) -> {
            List<BucketMetadata> bucketMetadata = modelGuidToBucketMetadataMap.get(modelGuid);
            boolean isRatingEngine = !modelGuid.equals(engineId);
            log.info("Updating bucket metadata for " + (isRatingEngine ? "engine " : "model ") + engineId + " to "
                    + JsonUtils.pprint(bucketMetadata));
        });

        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), configuration.getTargetTableName());
    }

    private void preDataFlow() {
        String inputTableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        ComputeLiftParameters params = new ComputeLiftParameters();
        params.setInputTableName(inputTableName);
        params.setLiftField(InterfaceName.Lift.name());
        params.setRatingField(ratingField);
        params.setModelGuidField(modelGuidField);
        params.setScoreFieldMap(getScoreFieldsMap());
        configuration.setDataFlowParams(params);
    }

    private Map<String, String> getScoreFieldsMap() {
        Map<String, String> scoreFieldsMap = getMapObjectFromContext(SCORING_SCORE_FIELDS, String.class, String.class);
        if (MapUtils.isEmpty(scoreFieldsMap)) {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            String scoreField = configuration.getScoreField();
            if (StringUtils.isBlank(scoreField)) {
                throw new IllegalArgumentException("Must specify score field for computing lift.");
            }
            scoreFieldsMap = ImmutableMap.of(modelGuid, scoreField);
        }
        return scoreFieldsMap;
    }

    private Map<String, String> getModelGuidToEngineIdMap() {
        Map<String, String> modelGuidToEngineIdMap = new HashMap<>();
        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        if (CollectionUtils.isNotEmpty(allContainers)) {
            List<RatingModelContainer> containers = allContainers.stream() //
                    .filter(container -> RatingEngineType.CROSS_SELL.equals(container.getEngineSummary().getType())) //
                    .collect(Collectors.toList());
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

    private Map<String, List<BucketMetadata>> getModelGuidToBucketMetadataMap() {
        Map<String, List<BucketMetadata>> modelGuidToBucketMetadataMap = new HashMap<>();
        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        if (CollectionUtils.isNotEmpty(allContainers)) {
            List<RatingModelContainer> containers = allContainers.stream() //
                    .filter(container -> RatingEngineType.CROSS_SELL.equals(container.getEngineSummary().getType())) //
                    .collect(Collectors.toList());
            containers.forEach(container -> {
                AIModel aiModel = (AIModel) container.getModel();
                String modelGuid = aiModel.getModelSummaryId();
                modelGuidToBucketMetadataMap.put(modelGuid, container.getEngineSummary().getBucketMetadata());
            });
        } else {
            String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);
            List<BucketMetadata> bucketMetadata = configuration.getBucketMetadata();
            modelGuidToBucketMetadataMap.put(modelGuid, bucketMetadata);
        }
        return modelGuidToBucketMetadataMap;
    }

}
