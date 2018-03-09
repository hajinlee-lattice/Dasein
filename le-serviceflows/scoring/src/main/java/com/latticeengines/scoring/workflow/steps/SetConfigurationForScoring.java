package com.latticeengines.scoring.workflow.steps;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RTSScoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("setConfigurationForScoring")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SetConfigurationForScoring extends BaseWorkflowStep<SetConfigurationForScoringConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SetConfigurationForScoring.class);

    @Inject
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    public SetConfigurationForScoring() {
    }

    @Override
    public void execute() {
        log.info("Setting the configuration for scoring.");
        log.info("Current name space is {}", namespace);
        String parentNamespace = getParentNamespace();
        log.info("parent name space is {}", parentNamespace);
        String matchStepNamespace = String.join(".", parentNamespace, rtsBulkScoreWorkflow.name(),
                matchDataCloudWorkflow.name(), MatchStepConfiguration.class.getSimpleName());
        MatchStepConfiguration matchStepConfig = (MatchStepConfiguration) getConfigurationFromJobParameters(
                matchStepNamespace);
        matchStepConfig.setSkipStep(true);
        Table matchResultTable = getObjectFromContext(MATCH_RESULT_TABLE, Table.class);
        matchStepConfig.setInputTableName(matchResultTable.getName());
        putObjectInContext(matchStepNamespace, matchStepConfig);

        String processMatchResultStepNamespace = String.join(".", parentNamespace, rtsBulkScoreWorkflow.name(),
                matchDataCloudWorkflow.name(), ProcessMatchResultConfiguration.class.getSimpleName());
        ProcessMatchResultConfiguration processMatchResultStepConfig = (ProcessMatchResultConfiguration) getConfigurationFromJobParameters(
                processMatchResultStepNamespace);
        processMatchResultStepConfig.setSkipStep(true);
        putObjectInContext(processMatchResultStepNamespace, processMatchResultStepConfig);

        String rtsScoreStepStepNamespace = String.join(".", parentNamespace, rtsBulkScoreWorkflow.name(),
                RTSScoreStepConfiguration.class.getSimpleName());
        RTSScoreStepConfiguration rtsScoreStepConfiguration = (RTSScoreStepConfiguration) getConfigurationFromJobParameters(
                rtsScoreStepStepNamespace);
        rtsScoreStepConfiguration.setModelId(getStringValueFromContext(SCORING_MODEL_ID));
        rtsScoreStepConfiguration.setModelType(getStringValueFromContext(SCORING_MODEL_TYPE));
        rtsScoreStepConfiguration.setInputTableName(matchResultTable.getName());
        log.info("rtsScoreStepConfiguration is ");
        putObjectInContext(rtsScoreStepStepNamespace, rtsScoreStepConfiguration);

        String exportStepStepNamespace = String.join(".", parentNamespace, rtsBulkScoreWorkflow.name(),
                ExportStepConfiguration.class.getSimpleName());
        ExportStepConfiguration exportStepConfiguration = (ExportStepConfiguration) getConfigurationFromJobParameters(
                exportStepStepNamespace);
        exportStepConfiguration.setUsingDisplayName(Boolean.TRUE);
        putObjectInContext(exportStepStepNamespace, exportStepConfiguration);

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
