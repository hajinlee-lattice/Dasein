
package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RatingEngineScoreWorkflow;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SetCdlConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.export.ExportWorkflow;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("setCdlConfigurationForScoring")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SetCdlConfigurationForScoring extends BaseWorkflowStep<SetCdlConfigurationForScoringConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SetCdlConfigurationForScoring.class);

    @Inject
    protected ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private RatingEngineScoreWorkflow ratingEngineScoreWorkflow;

    @Inject
    private ExportWorkflow exportWorkflow;

    @Override
    public void execute() {
        log.info("Setting the configuration for Cdl scoring.");

        String parentNamespace = namespace.substring(0, namespace.lastIndexOf('.'));
        String exportStepStepNamespace = String.join(".", parentNamespace, ratingEngineScoreWorkflow.name(),
                exportWorkflow.name(), ExportStepConfiguration.class.getSimpleName());
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
