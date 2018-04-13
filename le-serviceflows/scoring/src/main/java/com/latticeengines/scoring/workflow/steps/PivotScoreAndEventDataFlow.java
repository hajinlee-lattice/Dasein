package com.latticeengines.scoring.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
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
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("pivotScoreAndEventDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PivotScoreAndEventDataFlow extends RunDataFlow<PivotScoreAndEventConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PivotScoreAndEventDataFlow.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Override
    public void onConfigurationInitialized() {
        String scoreTableName = getStringValueFromContext(EXPORT_TABLE_NAME);

        PivotScoreAndEventParameters dataFlowParams = new PivotScoreAndEventParameters(scoreTableName);
        Map<String, Double> avgScores = getMapObjectFromContext(SCORING_AVG_SCORES, String.class, Double.class);
        if (MapUtils.isNotEmpty(avgScores)) {
            dataFlowParams.setAvgScores(avgScores);
        } else {
            dataFlowParams.setAvgScores(ImmutableMap.of(getStringValueFromContext(SCORING_MODEL_ID),
                    getDoubleValueFromContext(SCORING_AVG_SCORE))//
            );
        }
        Map<String, String> scoreFieldMap = getMapObjectFromContext(SCORING_SCORE_FIELDS, String.class, String.class);
        if (MapUtils.isNotEmpty(scoreFieldMap)) {
            dataFlowParams.setScoreFieldMap(scoreFieldMap);
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
        if (!targetExtractPath.endsWith(".avro")) {
            targetExtractPath = targetExtractPath.endsWith("/") ? targetExtractPath : targetExtractPath + "/";
            targetExtractPath += "*.avro";
        }
        saveBucketedScoreSummary(targetExtractPath);
        putOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_AVRO_PATH, targetExtractPath);

        putStringValueInContext(EXPORT_TABLE_NAME, configuration.getTargetTableName());
        String scoreOutputPath = getOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);
        String pivotOutputPath = StringUtils.replace(scoreOutputPath, "_scored_", "_pivoted_");
        putStringValueInContext(EXPORT_OUTPUT_PATH, pivotOutputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH, pivotOutputPath);
    }

    private void saveBucketedScoreSummary(String targetDataPath) {
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
        String customerSpace = configuration.getCustomerSpace().toString();
        Map<String, BucketedScoreSummary> bucketedScoreSummaryMap = new HashMap<>();
        pivotedRecordsMap.forEach((modelGuid, pivotedRecords) -> {
            BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                    .generateBucketedScoreSummary(pivotedRecords);
            if (configuration.isDeferSavingBucketedScoreSummaries()) {
                bucketedScoreSummaryMap.put(modelGuid, bucketedScoreSummary);
            } else {
                log.info("Save bucketed score summary for modelGUID=" + modelGuid + " : "
                        + JsonUtils.serialize(bucketedScoreSummary));
                bucketedScoreProxy.createOrUpdateBucketedScoreSummary(customerSpace, modelGuid, bucketedScoreSummary);
            }
        });
        if (configuration.isDeferSavingBucketedScoreSummaries()) {
            putObjectInContext(BUCKETED_SCORE_SUMMARIES, bucketedScoreSummaryMap);
        }
    }

}
