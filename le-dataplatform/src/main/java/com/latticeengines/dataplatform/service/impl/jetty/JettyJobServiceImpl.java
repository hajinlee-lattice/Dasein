package com.latticeengines.dataplatform.service.impl.jetty;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.impl.JobServiceImpl;
import com.latticeengines.dataplatform.service.jetty.JettyJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.jetty.JettyJob;

@Component("jettyJobService")
public class JettyJobServiceImpl extends JobServiceImpl implements JettyJobService {

    @Override
    public ApplicationId submitJob(JettyJob jettyJob) {
        ApplicationId appId = super.submitJob(jettyJob);
        return appId;
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        JobStatus jobStatus = new JobStatus();
        super.setJobStatus(jobStatus, applicationId);
        return jobStatus;
    }

}
