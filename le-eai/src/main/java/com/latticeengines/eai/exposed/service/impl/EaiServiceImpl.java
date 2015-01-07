package com.latticeengines.eai.exposed.service.impl;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.yarn.runtime.EaiContainerProperty;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

@Component("eaiService")
public class EaiServiceImpl implements EaiService {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(EaiServiceImpl.class);

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId extractAndImport(ImportConfiguration importConfig) {
        EaiJob eaiJob = createJob(importConfig);
        ApplicationId appId = jobService.submitJob(eaiJob);
        eaiJob.setId(appId.toString());
        jobEntityMgr.create(eaiJob);
        return appId;
    }

    private EaiJob createJob(ImportConfiguration importConfig) {
        EaiJob eaiJob = new EaiJob();

        String customer = importConfig.getCustomer();
        String targetPath = importConfig.getTargetPath();
        eaiJob.setClient("eaiClient");
        eaiJob.setCustomer(customer);
        eaiJob.setTargetPath(targetPath);
        //eaiJob.setTables(importConfig.toString());

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getNonMRQueueNameForSubmission(0));

        Properties containerProperties = new Properties();
        containerProperties.put(EaiContainerProperty.IMPORTCONFIG.name(), importConfig.toString());
        containerProperties.put(EaiContainerProperty.TARGET_PATH.name(), targetPath);
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "128");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        eaiJob.setAppMasterPropertiesObject(appMasterProperties);
        eaiJob.setContainerPropertiesObject(containerProperties);
        return eaiJob;
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return jobService.getJobStatus(applicationId);
    }

}
