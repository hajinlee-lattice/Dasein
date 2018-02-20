package com.latticeengines.modeling.workflow.steps;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RTSScoreStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("setConfigurationForScoring")
public class SetConfigurationForScoring extends BaseWorkflowStep<SetConfigurationForScoringConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SetConfigurationForScoring.class);

    @Override
    public void execute() {
        log.info("Setting the configuration for scoring.");
        MatchStepConfiguration matchStepConfig = getConfigurationFromJobParameters(MatchStepConfiguration.class);

        matchStepConfig.setSkipStep(true);
        Table matchResultTable = getObjectFromContext(MATCH_RESULT_TABLE, Table.class);
        matchStepConfig.setInputTableName(matchResultTable.getName());
        putObjectInContext(MatchStepConfiguration.class.getName(), matchStepConfig);
        
        ProcessMatchResultConfiguration processMatchResultStepConfig = getConfigurationFromJobParameters(
                ProcessMatchResultConfiguration.class);

        processMatchResultStepConfig.setSkipStep(true);
        putObjectInContext(ProcessMatchResultConfiguration.class.getName(), processMatchResultStepConfig);

        RTSScoreStepConfiguration rtsScoreStepConfiguration = getConfigurationFromJobParameters(
                RTSScoreStepConfiguration.class);
        rtsScoreStepConfiguration.setModelId(getStringValueFromContext(SCORING_MODEL_ID));
        rtsScoreStepConfiguration.setModelType(getStringValueFromContext(SCORING_MODEL_TYPE));
        rtsScoreStepConfiguration.setInputTableName(matchResultTable.getName());
        log.info("rtsScoreStepConfiguration is ");
        putObjectInContext(RTSScoreStepConfiguration.class.getName(), rtsScoreStepConfiguration);

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
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        String outputPath = String.format("%s/%s/data/%s/csv_files/score_event_table_output/%s",
                configuration.getModelingServiceHdfsBaseDir(), configuration.getCustomerSpace(), eventTable.getName(),
                targetFileName);
        putStringValueInContext(EXPORT_OUTPUT_PATH, outputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH,
                getStringValueFromContext(EXPORT_OUTPUT_PATH));

    }
}
