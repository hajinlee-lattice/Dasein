package com.latticeengines.eai.service.impl;

import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("eaiYarnService")
public class EaiYarnServiceImpl implements EaiYarnService {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Override
    public ApplicationId submitSingleYarnContainer(EaiJobConfiguration eaiJobConfig) {
        EaiJob eaiJob = createJob(eaiJobConfig);
        ApplicationId appId = jobService.submitJob(eaiJob);
        eaiJob.setId(appId.toString());
        jobEntityMgr.create(eaiJob);
        return appId;
    }

    private EaiJob createJob(EaiJobConfiguration eaiJobConfig) {
        EaiJob eaiJob = new EaiJob();
        StringBuilder customerSpace = new StringBuilder("");
        if (eaiJobConfig.getCustomerSpace() != null) {
            customerSpace.append(eaiJobConfig.getCustomerSpace().toString());
        } else {
            customerSpace.append(CustomerSpace.parse(this.getClass().getSimpleName()).toString());
        }
        eaiJob.setClient("eaiClient");
        eaiJob.setCustomer(customerSpace.toString());

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customerSpace.toString());
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getEaiQueueNameForSubmission());

        Properties containerProperties = new Properties();
        containerProperties.put(ImportProperty.EAICONFIG, eaiJobConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "128");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        eaiJob.setAppMasterPropertiesObject(appMasterProperties);
        eaiJob.setContainerPropertiesObject(containerProperties);
        return eaiJob;
    }

    @Override
    public ApplicationId submitMRJob(String mrJobName, Properties props) {
        return jobService.submitMRJob(mrJobName, props);
    }
}
