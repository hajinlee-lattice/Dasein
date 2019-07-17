package com.latticeengines.cdl.workflow.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.ScoreAggregateParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("scoreAggregateFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ScoreAggregateFlow extends RunDataFlow<ScoreAggregateFlowConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ScoreAggregateFlow.class);

    private static final String MODEL_GUID_FIELD = "Model_GUID";
    private static final String AVERAGE_SCORE_FIELD = InterfaceName.AverageScore.name();

    private boolean isMultiModel = false;

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        Table aggrTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration,
                aggrTable.getExtracts().get(0).getPath() + "/*.avro");
        if (isMultiModel) {
            postProcessMultiModel(records);
        } else {
            postProcessSingleModel(records);
        }
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), aggrTable.getName());
    }

    protected int getScalingMultiplier(double sizeInGb) {
        int multiplier = super.getScalingMultiplier(sizeInGb) * 4;
        log.info("Adjust multiplier=" + multiplier + " base on size=" + sizeInGb + " gb.");
        return multiplier;
    }

    private void postProcessSingleModel(List<GenericRecord> records) {
        log.info("Table=" + configuration.getTargetTableName() + " Average Score="
                + records.get(0).get(AVERAGE_SCORE_FIELD));
        putDoubleValueInContext(SCORING_AVG_SCORE, (Double) records.get(0).get(AVERAGE_SCORE_FIELD));
    }

    private void postProcessMultiModel(List<GenericRecord> records) {
        log.info("Table=" + configuration.getTargetTableName());
        Map<String, Double> avgScores = new HashMap<>();
        records.forEach(record -> {
            String modelGuid = record.get(MODEL_GUID_FIELD).toString();
            Double avgScore = (Double) record.get(AVERAGE_SCORE_FIELD);
            log.info("ModelGUID=" + modelGuid + ", AverageScore=" + avgScore);
            avgScores.put(modelGuid, avgScore);
        });
        putObjectInContext(SCORING_AVG_SCORES, avgScores);
    }

    private void setupDataFlow() {
        ScoreAggregateParameters params = new ScoreAggregateParameters();
        params.setScoreResultsTableName(getScoreResultTableName());
        params.setExpectedValue(configuration.getExpectedValue());

        List<RatingModelContainer> containers = getModelContainers();
        if (CollectionUtils.isNotEmpty(containers)) {
            isMultiModel = true;
            Map<String, String> scoreFieldMap = new HashMap<>();
            containers.forEach(container -> {
                AIModel model = (AIModel) container.getModel();
                String modelGuid = model.getModelSummaryId();
                PredictionType predictionType = model.getPredictionType();
                if (PredictionType.EXPECTED_VALUE.equals(predictionType)) {
                    scoreFieldMap.put(modelGuid, InterfaceName.ExpectedRevenue.name());
                } else {
                    scoreFieldMap.put(modelGuid, InterfaceName.Probability.name());
                }
            });
            params.setScoreFieldMap(scoreFieldMap);
            params.setModelGuidField(MODEL_GUID_FIELD);
        }
        configuration.setDataFlowParams(params);
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTableName == null) {
            scoreResultTableName = getDataFlowParams().getScoreResultsTableName();
        }
        return scoreResultTableName;
    }

    private ScoreAggregateParameters getDataFlowParams() {
        return (ScoreAggregateParameters) configuration.getDataFlowParams();
    }

    private List<RatingModelContainer> getModelContainers() {
        List<RatingModelContainer> allContainers = getListObjectFromContext(ITERATION_RATING_MODELS, RatingModelContainer.class);
        if (CollectionUtils.isEmpty(allContainers)) {
            return Collections.emptyList();
        }
        return allContainers.stream() //
                .filter(container -> {
                    RatingEngineType ratingEngineType = container.getEngineSummary().getType();
                    return RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                            || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType);
                }) //
                .collect(Collectors.toList());
    }

}
