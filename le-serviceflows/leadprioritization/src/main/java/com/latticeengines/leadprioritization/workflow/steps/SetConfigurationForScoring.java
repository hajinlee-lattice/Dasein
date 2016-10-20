package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

@Component("setConfigurationForScoring")
public class SetConfigurationForScoring extends BaseWorkflowStep<SetConfigurationForScoringConfiguration> {

    @Override
    public void execute() {
        MatchStepConfiguration matchStepConfig = getConfigurationFromJobParameters(MatchStepConfiguration.class);
        ModelStepConfiguration modelStepConfiguration = getConfigurationFromJobParameters(ModelStepConfiguration.class);
        matchStepConfig.setInputTableName(modelStepConfiguration.getTrainingTableName());

        putObjectInContext(MatchStepConfiguration.class.getName(), matchStepConfig);

        ScoreStepConfiguration scoreStepConfiguration = getConfigurationFromJobParameters(ScoreStepConfiguration.class);
        scoreStepConfiguration.setModelId(getStringValueFromContext(SCORING_MODEL_ID));
        putObjectInContext(ScoreStepConfiguration.class.getName(), scoreStepConfiguration);

        ExportStepConfiguration exportStepConfiguration = getConfigurationFromJobParameters(ExportStepConfiguration.class);
        exportStepConfiguration.setUsingDisplayName(Boolean.TRUE);
        String sourceFileName = configuration.getInputProperties().get(
                WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME);
        String targetFileName = String.format("%s_scored_%s", StringUtils.substringBeforeLast(
                sourceFileName.replaceAll("[^A-Za-z0-9_]", "_"), ".csv"), DateTime.now().getMillis());
        // exportStepConfiguration.putProperty(ExportProperty.TARGET_FILE_NAME,
        // targetFileName);
        putObjectInContext(ExportStepConfiguration.class.getName(), exportStepConfiguration);

        putStringValueInContext(EXPORT_INPUT_PATH, "");
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        String outputPath = String.format("%s/%s/data/%s/csv_files/score_event_table_output/%s",
                configuration.getModelingServiceHdfsBaseDir(), configuration.getCustomerSpace(), eventTable.getName(), targetFileName);
        putStringValueInContext(EXPORT_OUTPUT_PATH, outputPath);
    }

}
