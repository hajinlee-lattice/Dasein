package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("pivotScoreAndEventDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PivotScoreAndEventDataFlow extends RunDataFlow<PivotScoreAndEventConfiguration> {

    private Logger log = LoggerFactory.getLogger(PivotScoreAndEventDataFlow.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Override
    public void onConfigurationInitialized() {
        String scoreTableName = getStringValueFromContext(EXPORT_TABLE_NAME);

        PivotScoreAndEventParameters dataFlowParams = new PivotScoreAndEventParameters(scoreTableName);
        Map<String, Double> avgScores = getMapObjectFromContext(SCORING_AVG_SCORES, String.class, Double.class);
        if (avgScores != null) {
            dataFlowParams.setAvgScores(avgScores);
        } else {
            dataFlowParams.setAvgScores(ImmutableMap.of(getStringValueFromContext(SCORING_MODEL_ID),
                    getDoubleValueFromContext(SCORING_AVG_SCORE))//
            );
        }
        Map<String, PredictionType> predictionTypes = getMapObjectFromContext(PREDICTION_TYPES, String.class,
                PredictionType.class);
        if (predictionTypes != null) {
            dataFlowParams.setScoreFieldMap(predictionTypes.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> PredictionType.EXPECTED_VALUE == e.getValue()
                            ? InterfaceName.ExpectedRevenue.name() : InterfaceName.RawScore.name())));
        } else {
            dataFlowParams.setScoreFieldMap(
                    ImmutableMap.of(getStringValueFromContext(SCORING_MODEL_ID), configuration.isExpectedValue()
                            ? InterfaceName.ExpectedRevenue.name() : InterfaceName.RawScore.name()));
        }
        configuration.setDataFlowParams(dataFlowParams);
        configuration.setTargetTableName(scoreTableName + "_pivot");
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, eventTable);
        String targetExtractPath = eventTable.getExtracts().get(0).getPath();
        // saveBucketedScoreSummary(targetExtractPath,
        // configuration.getCustomerSpace());
        putOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_AVRO_PATH, targetExtractPath);

        putStringValueInContext(EXPORT_TABLE_NAME, configuration.getTargetTableName());
        String scoreOutputPath = getOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);
        String pivotOutputPath = StringUtils.replace(scoreOutputPath, "_scored_", "_pivoted_");
        putStringValueInContext(EXPORT_OUTPUT_PATH, pivotOutputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH, pivotOutputPath);
        try {
            // TODO: remove this in the future
            internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
            List<BucketMetadata> bucketMetadatas = null;
            if (configuration.getRatingEngineId() != null && configuration.getModelId() != null) {
                log.info("Creating CDL bucket metadata.");
                bucketMetadatas = internalResourceRestApiProxy.createDefaultABCDBuckets(
                        CustomerSpace.parse(configuration.getCustomerSpace().toString()).toString(),
                        configuration.getRatingEngineId(), configuration.getModelId(), configuration.getUserId(), false,
                        false);
            } else {
                log.info("Creating LPI bucket metadata.");
                bucketMetadatas = internalResourceRestApiProxy.createDefaultABCDBuckets(
                        getStringValueFromContext(SCORING_MODEL_ID), configuration.getUserId(), false, false, false);
            }

            log.info(String.format("Created A bucket (%s - %s) with %s leads and %s lift,"
                    + "B bucket (%s - %s) with %s leads and %s lift," + "C bucket (%s - %s) with %s leads and %s lift,"
                    + "D bucket (%s - %s) with %s leads and %s lift", bucketMetadatas.get(0).getLeftBoundScore(),
                    bucketMetadatas.get(0).getRightBoundScore(), bucketMetadatas.get(0).getNumLeads(),
                    bucketMetadatas.get(0).getLift(), bucketMetadatas.get(1).getLeftBoundScore(),
                    bucketMetadatas.get(1).getRightBoundScore(), bucketMetadatas.get(1).getNumLeads(),
                    bucketMetadatas.get(1).getLift(), bucketMetadatas.get(2).getLeftBoundScore(),
                    bucketMetadatas.get(2).getRightBoundScore(), bucketMetadatas.get(2).getNumLeads(),
                    bucketMetadatas.get(2).getLift(), bucketMetadatas.get(3).getLeftBoundScore(),
                    bucketMetadatas.get(3).getRightBoundScore(), bucketMetadatas.get(3).getNumLeads(),
                    bucketMetadatas.get(3).getLift()));
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            log.warn(String.format("Creating default ABCD buckets for model: %s failed. Proceeding with the workflow",
                    getStringValueFromContext(SCORING_MODEL_ID)));
        }
    }

    private void saveBucketedScoreSummary(String targetDataPath, CustomerSpace customerSpace) {
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, targetDataPath);
        Map<String, BucketedScoreSummary> bucketedScoreSummaryMap = new HashMap<>();
        String prevModelGuid = "";
        List<GenericRecord> pivotedRecords = new ArrayList<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            if (bucketedScoreSummaryMap.containsKey(modelGuid)) {
                pivotedRecords.add(record);
            } else {
                if (StringUtils.isNotEmpty(prevModelGuid)) {
                    BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                            .generateBucketedScoreSummary(pivotedRecords);
                    bucketedScoreSummaryMap.put(prevModelGuid, bucketedScoreSummary);
                }
                prevModelGuid = modelGuid;
                pivotedRecords.clear();
            }
        }
        bucketedScoreSummaryMap.entrySet().forEach(
                e -> internalResourceRestApiProxy.createBucketedScoreSummary(e.getKey(), customerSpace, e.getValue()));
    }

}
