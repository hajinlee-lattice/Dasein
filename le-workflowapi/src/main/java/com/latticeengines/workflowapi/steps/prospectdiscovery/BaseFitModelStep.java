package com.latticeengines.workflowapi.steps.prospectdiscovery;

import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
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

    protected RestTemplate restTemplate = new RestTemplate();

    protected void waitForAppId(String appId, String microServiceHostPort) {
        JobStatus status;
        int maxTries = 1000;
        int i = 0;
        do {
            String url = String.format(microServiceHostPort + "/modeling/jobs/%s", appId);
            status = restTemplate.getForObject(url, JobStatus.class);
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
            i++;

            if (i == maxTries) {
                break;
            }
        } while (!TERMINAL_STATUS.contains(status));
    }

}
