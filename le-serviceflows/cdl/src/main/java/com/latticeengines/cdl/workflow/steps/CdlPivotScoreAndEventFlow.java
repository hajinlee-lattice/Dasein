package com.latticeengines.cdl.workflow.steps;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CdlPivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CdlPivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("cdlPivotScoreAndEventDataFlow")
public class CdlPivotScoreAndEventFlow extends RunDataFlow<CdlPivotScoreAndEventConfiguration> {

    private Logger log = LoggerFactory.getLogger(CdlPivotScoreAndEventFlow.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Override
    public void onConfigurationInitialized() {
        String scoreTableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        CdlPivotScoreAndEventParameters dataFlowParams = new CdlPivotScoreAndEventParameters(scoreTableName);
        dataFlowParams.setAvgScore(getDoubleValueFromContext(SCORING_AVG_SCORE));
        dataFlowParams.setExpectedValue(configuration.isExpectedValue());
        configuration.setDataFlowParams(dataFlowParams);
        configuration.setTargetTableName(scoreTableName + "_pivot");
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, eventTable);
        putOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_AVRO_PATH,
                eventTable.getExtracts().get(0).getPath());

        putStringValueInContext(EXPORT_TABLE_NAME, configuration.getTargetTableName());
        String scoreOutputPath = getOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);
        String pivotOutputPath = StringUtils.replace(scoreOutputPath, "_scored_", "_pivoted_");
        putStringValueInContext(EXPORT_OUTPUT_PATH, pivotOutputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH, pivotOutputPath);
        try {
            internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
            List<BucketMetadata> bucketMetadatas = internalResourceRestApiProxy.createDefaultABCDBuckets(
                    getStringValueFromContext(SCORING_MODEL_ID), configuration.getUserId(), true,
                    false, false);

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

}
