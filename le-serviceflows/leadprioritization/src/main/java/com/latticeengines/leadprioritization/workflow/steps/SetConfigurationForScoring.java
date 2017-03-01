package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.ProcessMatchResultConfiguration;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

@Component("setConfigurationForScoring")
public class SetConfigurationForScoring extends BaseWorkflowStep<SetConfigurationForScoringConfiguration> {

    @Override
    public void execute() {
        MatchStepConfiguration matchStepConfig = getConfigurationFromJobParameters(MatchStepConfiguration.class);
        Table eventTable = getObjectFromContext(MATCH_RESULT_TABLE, Table.class);
        matchStepConfig.setInputTableName(eventTable.getName());
        matchStepConfig.setSkipStep(true);
        putObjectInContext(MatchStepConfiguration.class.getName(), matchStepConfig);

        ProcessMatchResultConfiguration processMatchResultStepConfig = getConfigurationFromJobParameters(
                ProcessMatchResultConfiguration.class);
        processMatchResultStepConfig.setSkipStep(true);
        putObjectInContext(ProcessMatchResultConfiguration.class.getName(), processMatchResultStepConfig);

        DedupEventTableConfiguration dedupStepConfig = getConfigurationFromJobParameters(
                DedupEventTableConfiguration.class);
        if (dedupStepConfig.isSkipStep()) {
            AddStandardAttributesConfiguration addStandardAttrStepConfig = getConfigurationFromJobParameters(
                    AddStandardAttributesConfiguration.class);
            addStandardAttrStepConfig.setSkipStep(true);
            putObjectInContext(AddStandardAttributesConfiguration.class.getName(), addStandardAttrStepConfig);
        }

        ScoreStepConfiguration scoreStepConfiguration = getConfigurationFromJobParameters(ScoreStepConfiguration.class);
        scoreStepConfiguration.setModelId(getStringValueFromContext(SCORING_MODEL_ID));
        putObjectInContext(ScoreStepConfiguration.class.getName(), scoreStepConfiguration);

        ExportStepConfiguration exportStepConfiguration = getConfigurationFromJobParameters(
                ExportStepConfiguration.class);
        exportStepConfiguration.setUsingDisplayName(Boolean.TRUE);
        putObjectInContext(ExportStepConfiguration.class.getName(), exportStepConfiguration);

        putStringValueInContext(EXPORT_INPUT_PATH, "");
        String sourceFileName = configuration.getInputProperties()
                .get(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME);
        String targetFileName = String.format("%s_scored_%s",
                StringUtils.substringBeforeLast(sourceFileName.replaceAll("[^A-Za-z0-9_]", "_"), ".csv"),
                DateTime.now().getMillis());
        eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        String outputPath = String.format("%s/%s/data/%s/csv_files/score_event_table_output/%s",
                configuration.getModelingServiceHdfsBaseDir(), configuration.getCustomerSpace(), eventTable.getName(),
                targetFileName);
        putStringValueInContext(EXPORT_OUTPUT_PATH, outputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH,
                getStringValueFromContext(EXPORT_OUTPUT_PATH));

    }
}
