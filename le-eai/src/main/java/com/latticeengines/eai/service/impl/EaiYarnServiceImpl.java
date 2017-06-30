package com.latticeengines.eai.service.impl;

import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.BaseContext;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("eaiYarnService")
public class EaiYarnServiceImpl implements EaiYarnService {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Override
    public ApplicationId submitSingleYarnContainerJob(EaiJobConfiguration eaiJobConfig) {
        EaiJob eaiJob = createJob(eaiJobConfig);
        ApplicationId appId = jobService.submitJob(eaiJob);
        eaiJob.setId(appId.toString());
        jobEntityMgr.create(eaiJob);
        return appId;
    }

    @Override
    public void submitSingleYarnContainerJob(EaiJobConfiguration eaiJobConfig, BaseContext context) {
        ApplicationId appId = submitSingleYarnContainerJob(eaiJobConfig);
        context.setProperty(ExportProperty.APPID, appId);
    }

    @Override
    public ApplicationId submitMRJob(String mrJobName, Properties props) {
        return jobService.submitMRJob(mrJobName, props);
    }
}
