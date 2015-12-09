package com.latticeengines.serviceflows.workflow.core;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class BaseWorkflowStep<T> extends AbstractStep<T> {

    private static final Log log = LogFactory.getLog(BaseWorkflowStep.class);

    protected static final String PREMATCH_EVENT_TABLE = "PREMATCH_EVENT_TABLE";
    protected static final String EVENT_TABLE = "EVENT_TABLE";
    protected static final String DB_CREDS = "DB_CREDS";
    protected static final String MATCH_COMMAND_ID = "MATCH_COMMAND_ID";
    protected static final String MODELING_SERVICE_EXECUTOR_BUILDER = "MODELING_SERVICE_EXECUTOR_BUILDER";
    protected static final String MODEL_APP_IDS = "MODEL_APP_IDS";

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
        log.info(String.format("Waiting for appId: %s", appId));

        JobStatus status;
        int maxTries = 10000;
        int i = 0;
        do {
            String url = String.format(microServiceHostPort + "/modeling/jobs/%s", appId);
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
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));

        if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
            throw new LedpException(LedpCode.LEDP_28015, new String[] { appId, status.getStatus().toString() });
        }

    }

    protected ModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            ModelStepConfiguration modelStepConfiguration, Table eventTable) {
        String metadataContents = JsonUtils.serialize(eventTable.getModelingMetadata());

        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.sampleSubmissionUrl("/modeling/samples") //
                .profileSubmissionUrl("/modeling/profiles") //
                .modelSubmissionUrl("/modeling/models") //
                .retrieveFeaturesUrl("/modeling/features") //
                .retrieveJobStatusUrl("/modeling/jobs/%s") //
                .retrieveModelingJobStatusUrl("/modeling/modelingjobs/%s") //
                .modelingServiceHostPort(modelStepConfiguration.getMicroServiceHostPort()) //
                .modelingServiceHdfsBaseDir(modelStepConfiguration.getModelingServiceHdfsBaseDir()) //
                .customer(modelStepConfiguration.getCustomerSpace().toString()) //
                .metadataContents(metadataContents) //
                .yarnConfiguration(yarnConfiguration) //
                .hdfsDirToSample(eventTable.getExtracts().get(0).getPath()) //
                .table(eventTable.getName());

        return bldr;
    }

}
