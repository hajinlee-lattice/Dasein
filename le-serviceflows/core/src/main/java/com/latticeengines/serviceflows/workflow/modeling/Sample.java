package com.latticeengines.serviceflows.workflow.modeling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("sample")
public class Sample extends BaseModelStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(Sample.class);

    @Autowired
    private MetadataProxy metadataProxy;

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
