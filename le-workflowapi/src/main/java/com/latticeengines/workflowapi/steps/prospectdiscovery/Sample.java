package com.latticeengines.workflowapi.steps.prospectdiscovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("sample")
public class Sample extends BaseFitModelStep<BaseFitModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(Sample.class);

    @Override
    public void execute() {
        log.info("Inside Sample execute()");

        Table eventTable = JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);

        ModelingServiceExecutor.Builder bldr;
        try {
            bldr = sample(eventTable);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28006, e, new String[] { eventTable.getName() });
        }

        executionContext.putString(MODELING_SERVICE_EXECUTOR_BUILDER, JsonUtils.serialize(bldr));
    }

    private ModelingServiceExecutor.Builder sample(Table eventTable) throws Exception {
        String metadataContents = JsonUtils.serialize(eventTable.getModelingMetadata());
        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.sampleSubmissionUrl("/modeling/samples") //
                .profileSubmissionUrl("/modeling/profiles") //
                .modelSubmissionUrl("/modeling/models") //
                .retrieveFeaturesUrl("/modeling/features") //
                .retrieveJobStatusUrl("/modeling/jobs/%s") //
                .modelingServiceHostPort(configuration.getMicroServiceHostPort()) //
                .modelingServiceHdfsBaseDir(configuration.getModelingServiceHdfsBaseDir()) //
                .customer(configuration.getCustomerSpace()) //
                .metadataContents(metadataContents) //
                .yarnConfiguration(yarnConfiguration) //
                .hdfsDirToSample(eventTable.getExtracts().get(0).getPath()) //
                .table(eventTable.getName());

        ModelingServiceExecutor modelExecutor = new ModelingServiceExecutor(bldr);
        modelExecutor.sample();
        return bldr;
    }

}
