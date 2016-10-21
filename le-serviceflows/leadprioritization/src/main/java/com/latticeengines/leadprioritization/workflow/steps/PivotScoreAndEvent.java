package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;

@Component("pivotScoreAndEvent")
public class PivotScoreAndEvent extends RunDataFlow<PivotScoreAndEventConfiguration> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void onConfigurationInitialized() {
        String scoreTableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        Double modelAvgProbability = getDoubleValueFromContext(MODEL_AVG_PROBABILITY);
        if (modelAvgProbability == null) {
            throw new RuntimeException("ModelAvgProbability is null!");
        }
        configuration.setDataFlowParams(new PivotScoreAndEventParameters(scoreTableName, modelAvgProbability));
        String exportTableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        configuration.setTargetTableName(exportTableName + "_pivot");
    }

    @Override
    public void onExecutionCompleted() {
//        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
//                configuration.getTargetTableName());
//        putObjectInContext(EVENT_TABLE, eventTable);

        ExportStepConfiguration exportStepConfiguration = getConfigurationFromJobParameters(ExportStepConfiguration.class);
        exportStepConfiguration.setUsingDisplayName(Boolean.TRUE);
        String targetFileName = String.format("score_event_pivot_%d.csv", DateTime.now().getMillis());
        exportStepConfiguration.putProperty(ExportProperty.TARGET_FILE_NAME, targetFileName);
        putObjectInContext(ExportStepConfiguration.class.getName(), exportStepConfiguration);
        putStringValueInContext(EXPORT_TABLE_NAME, configuration.getTargetTableName());

        putStringValueInContext(EXPORT_INPUT_PATH, "");
        String outputPath = String.format("%s/%s",
                StringUtils.substringBeforeLast(getStringValueFromContext(EXPORT_OUTPUT_PATH), "/"), targetFileName);
        putStringValueInContext(EXPORT_OUTPUT_PATH, outputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH,
                getStringValueFromContext(EXPORT_OUTPUT_PATH));
    }
}
