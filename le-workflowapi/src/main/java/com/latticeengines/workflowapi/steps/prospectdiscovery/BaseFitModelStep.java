package com.latticeengines.workflowapi.steps.prospectdiscovery;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class BaseFitModelStep<T> extends AbstractStep<T> {

    protected static final String PREMATCH_EVENT_TABLE = "PREMATCH_EVENT_TABLE";
    protected static final String EVENT_TABLE = "EVENT_TABLE";
    protected static final String DB_CREDS = "DB_CREDS";
    protected static final String MATCH_COMMAND_ID = "MATCH_COMMAND_ID";
    protected static final String MODELING_SERVICE_EXECUTOR_BUILDER = "MODELING_SERVICE_EXECUTOR_BUILDER";

    private static final EnumSet<FinalApplicationStatus> TERMINAL_STATUS = EnumSet.of(FinalApplicationStatus.FAILED,
            FinalApplicationStatus.KILLED, FinalApplicationStatus.SUCCEEDED);

    @Autowired
    protected Configuration yarnConfiguration;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
    protected List<ClientHttpRequestInterceptor> addMagicAuthHeaders = Arrays
            .asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader });
    protected RestTemplate restTemplate = new RestTemplate();

    @Override
    public boolean setup() {
        boolean result = super.setup();
        restTemplate.setInterceptors(addMagicAuthHeaders);

        return result;
    }

    protected void waitForAppId(String appId, String microServiceHostPort) {
        JobStatus status;
        int maxTries = 10000;
        int i = 0;
        do {
            String url = String.format(microServiceHostPort + "/scoring/scoringJobs/%s", appId);
            status = restTemplate.getForObject(url, JobStatus.class);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
            i++;

            if (i == maxTries) {
                break;
            }
        } while (!TERMINAL_STATUS.contains(status.getStatus()));
    }

    protected ModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            BaseFitModelStepConfiguration baseFitModelStepConfiguration, Table eventTable) {
        String metadataContents = JsonUtils.serialize(eventTable.getModelingMetadata());

        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.sampleSubmissionUrl("/modeling/samples") //
                .profileSubmissionUrl("/modeling/profiles") //
                .modelSubmissionUrl("/modeling/models") //
                .retrieveFeaturesUrl("/modeling/features") //
                .retrieveJobStatusUrl("/modeling/jobs/%s") //
                .modelingServiceHostPort(baseFitModelStepConfiguration.getMicroServiceHostPort()) //
                .modelingServiceHdfsBaseDir(baseFitModelStepConfiguration.getModelingServiceHdfsBaseDir()) //
                .customer(baseFitModelStepConfiguration.getCustomerSpace()) //
                .metadataContents(metadataContents) //
                .yarnConfiguration(yarnConfiguration) //
                .hdfsDirToSample(eventTable.getExtracts().get(0).getPath()) //
                .table(eventTable.getName());

        return bldr;
    }

}
