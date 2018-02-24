package com.latticeengines.scoring.workflow.steps;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.ComputeLiftParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ComputeLiftDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("computeLiftDataFlow")
public class ComputeLiftDataFlow extends RunDataFlow<ComputeLiftDataFlowConfiguration> {

    private static final String modelGuidField = "Model_GUID";
    private static final String ratingField = ScoreResultField.Rating.displayName;
    private static final String liftField = InterfaceName.Lift.name();

    @Override
    public void execute() {
        preDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        Map<String, String> scoreFieldMap = getMapObjectFromContext(SCORING_SCORE_FIELDS, String.class, String.class);

        Table liftTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration,
                liftTable.getExtracts().get(0).getPath() + "/*.avro");

        Map<String, Map<String, Double>> liftMap = new HashMap<>();
        Map<String, String> modelGuidToEngineIdMap = getModelGuidToEngineIdMap();
        records.forEach(record -> {
            String modelGuid = record.get(modelGuidField).toString();
            String rating = record.get(ratingField).toString();
            Double lift = (Double) record.get(liftField);
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

        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), configuration.getTargetTableName());
    }

    private void preDataFlow() {
        String inputTableName = getStringValueFromContext(AI_RAW_RATING_TABLE_NAME);
        ComputeLiftParameters params = new ComputeLiftParameters();
        params.setInputTableName(inputTableName);
        params.setLiftField(InterfaceName.Lift.name());
        params.setRatingField(ratingField);
        params.setModelGuidField(modelGuidField);
        params.setScoreFieldMap(getMapObjectFromContext(SCORING_SCORE_FIELDS, String.class, String .class));
        configuration.setDataFlowParams(params);
    }

    private Map<String, String> getModelGuidToEngineIdMap() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        List<RatingModelContainer> containers = allContainers.stream() //
                .filter(container -> RatingEngineType.AI_BASED.equals(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
        Map<String, String> modelGuidToEngineIdMap = new HashMap<>();
        containers.forEach(container -> {
            AIModel aiModel = (AIModel) container.getModel();
            String modelGuid = aiModel.getModelGuid();
            String engineId = container.getEngineSummary().getId();
            modelGuidToEngineIdMap.put(modelGuid, engineId);
        });
        return modelGuidToEngineIdMap;
    }

}
