package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.export.ExportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.ProcessMatchResultConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

@Component("setConfigurationForScoring")
public class SetConfigurationForScoring extends BaseWorkflowStep<SetConfigurationForScoringConfiguration> {

    @Override
    public void execute() {
        DedupEventTableConfiguration dedupStepConfig = getConfigurationFromJobParameters(DedupEventTableConfiguration.class);
        if (!dedupStepConfig.isSkipStep()) {
            MatchStepConfiguration matchStepConfig = getConfigurationFromJobParameters(MatchStepConfiguration.class);
            ModelStepConfiguration modelStepConfiguration = getConfigurationFromJobParameters(ModelStepConfiguration.class);
            matchStepConfig.setInputTableName(modelStepConfiguration.getTrainingTableName());

            putObjectInContext(MatchStepConfiguration.class.getName(), matchStepConfig);
        } else {
            MatchStepConfiguration matchStepConfig = getConfigurationFromJobParameters(MatchStepConfiguration.class);
            Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
            matchStepConfig.setInputTableName(eventTable.getName());
            matchStepConfig.setSkipStep(true);
            putObjectInContext(MatchStepConfiguration.class.getName(), matchStepConfig);

            ProcessMatchResultConfiguration processMatchResultStepConfig = getConfigurationFromJobParameters(ProcessMatchResultConfiguration.class);
            processMatchResultStepConfig.setSkipStep(true);
            putObjectInContext(ProcessMatchResultConfiguration.class.getName(), processMatchResultStepConfig);

            AddStandardAttributesConfiguration addStandardAttrStepConfig = getConfigurationFromJobParameters(AddStandardAttributesConfiguration.class);
            addStandardAttrStepConfig.setSkipStep(true);
            putObjectInContext(AddStandardAttributesConfiguration.class.getName(), addStandardAttrStepConfig);
        }

        ScoreStepConfiguration scoreStepConfiguration = getConfigurationFromJobParameters(ScoreStepConfiguration.class);
        scoreStepConfiguration.setModelId(getStringValueFromContext(SCORING_MODEL_ID));
        putObjectInContext(ScoreStepConfiguration.class.getName(), scoreStepConfiguration);

        ExportStepConfiguration exportStepConfiguration = getConfigurationFromJobParameters(ExportStepConfiguration.class);
        exportStepConfiguration.setUsingDisplayName(Boolean.TRUE);
        String sourceFileName = configuration.getInputProperties().get(
                WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME);
        String targetFileName = String.format("%s_scored_%s", StringUtils.substringBeforeLast(
                sourceFileName.replaceAll("[^A-Za-z0-9_]", "_"), ".csv"), DateTime.now().getMillis());
        exportStepConfiguration.putProperty(ExportProperty.TARGET_FILE_NAME, targetFileName);
        putObjectInContext(ExportStepConfiguration.class.getName(), exportStepConfiguration);

        putStringValueInContext(EXPORT_INPUT_PATH, "");
        putStringValueInContext(EXPORT_OUTPUT_PATH, "");

    }
}
