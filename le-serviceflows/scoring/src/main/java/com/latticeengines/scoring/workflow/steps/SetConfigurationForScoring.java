package com.latticeengines.scoring.workflow.steps;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("setConfigurationForScoring")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SetConfigurationForScoring extends BaseWorkflowStep<SetConfigurationForScoringConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SetConfigurationForScoring.class);

    public SetConfigurationForScoring() {
    }

    @Override
    public void execute() {
        log.info("Setting the configuration for scoring.");

        // for export step in score workflow
        removeObjectFromContext(EXPORT_INPUT_PATH);
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

        // for score step in score workflow
        Table matchResultTable = getObjectFromContext(MATCH_RESULT_TABLE, Table.class);
        putObjectInContext(EVENT_TABLE, matchResultTable);

    }

}
