package com.latticeengines.serviceflows.workflow.modeling;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("sample")
public class Sample extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(Sample.class);

    @Override
    public void execute() {
        log.info("Inside Sample execute()");

        Table eventTable = getEventTable();
        try {
            sample(eventTable);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28006, e, new String[] { eventTable.getName() });
        }
        putStringValueInContext(EXPORT_TABLE_NAME, eventTable.getName());
        String inputPath = configuration.getModelingServiceHdfsBaseDir() + configuration.getCustomerSpace() + "/data/"
                + eventTable.getName() + "/samples";
        putStringValueInContext(EXPORT_INPUT_PATH, inputPath);

        String outputPath = configuration.getModelingServiceHdfsBaseDir() + configuration.getCustomerSpace() + "/data/"
                + eventTable.getName() + "/csv_files/postMatchEventTable";
        putStringValueInContext(EXPORT_OUTPUT_PATH, outputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.POST_MATCH_EVENT_TABLE_EXPORT_PATH,
                getStringValueFromContext(EXPORT_OUTPUT_PATH));
    }

    private void sample(Table eventTable) throws Exception {
        ModelingServiceExecutor.Builder bldr = createModelingServiceExecutorBuilder(configuration, eventTable);
        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        modelExecutor.sample();
    }

}
