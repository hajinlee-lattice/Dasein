package com.latticeengines.dataplatform.service.impl.eai;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.eai.EaiJobService;
import com.latticeengines.dataplatform.service.impl.JobServiceImpl;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.EaiJob;

@Component("eaiJobService")
public class EaiJobServiceImpl extends JobServiceImpl implements EaiJobService {
    @Autowired
    private JobNameService jobNameService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId submitJob(EaiJob eaiJob) {
        ApplicationId appId = super.submitJob(eaiJob);
        eaiJob.setId(appId.toString());
        jobEntityMgr.create(eaiJob);
        return appId;
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        JobStatus jobStatus = new JobStatus();
        super.setJobStatus(jobStatus, applicationId);
        return jobStatus;
    }
}
