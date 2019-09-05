package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ScoreAggregateJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.score.ScoreAggregateJob;

@Component("scoreAggregateFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ScoreAggregateFlow extends RunSparkJob<ScoreAggregateFlowConfiguration, ScoreAggregateJobConfig> {
    private static final Logger log = LoggerFactory.getLogger(ScoreAggregateFlow.class);

    private static final String MODEL_GUID_FIELD = "Model_GUID";
    private static final String AVERAGE_SCORE_FIELD = InterfaceName.AverageScore.name();

    private boolean isMultiModel = false;

    @Override
    protected Class<ScoreAggregateJob> getJobClz() {
        return ScoreAggregateJob.class;
    }

    @Override
    protected ScoreAggregateJobConfig configureJob(ScoreAggregateFlowConfiguration stepConfiguration) {
    
        ScoreAggregateJobConfig jobConfig = new ScoreAggregateJobConfig();
        jobConfig.scoreResultsTableName = getScoreResultTableName();
        Table scoreResultTable = metadataProxy.getTable(customerSpace.toString(), jobConfig.scoreResultsTableName);
        jobConfig.expectedValue = configuration.getExpectedValue();
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
            jobConfig.scoreFieldMap = scoreFieldMap;
            jobConfig.modelGuidField = MODEL_GUID_FIELD;
        }
        
        List<DataUnit> inputUnits = new ArrayList<>();
        inputUnits.add(scoreResultTable.toHdfsDataUnit("scoreResult"));
        jobConfig.setInput(inputUnits);
        return jobConfig;
    }
    
    @Override
    protected void postJobExecution(SparkJobResult result) {
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration,
                result.getTargets().get(0).getPath() + "/*.avro");
        if (isMultiModel) {
            postProcessMultiModel(records);
        } else {
            postProcessSingleModel(records);
        }
    }

    private void postProcessSingleModel(List<GenericRecord> records) {
        putDoubleValueInContext(SCORING_AVG_SCORE, (Double) records.get(0).get(AVERAGE_SCORE_FIELD));
    }

    private void postProcessMultiModel(List<GenericRecord> records) {
        Map<String, Double> avgScores = new HashMap<>();
        records.forEach(record -> {
            String modelGuid = record.get(MODEL_GUID_FIELD).toString();
            Double avgScore = (Double) record.get(AVERAGE_SCORE_FIELD);
            log.info("ModelGUID=" + modelGuid + ", AverageScore=" + avgScore);
            avgScores.put(modelGuid, avgScore);
        });
        putObjectInContext(SCORING_AVG_SCORES, avgScores);
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        return scoreResultTableName;
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
