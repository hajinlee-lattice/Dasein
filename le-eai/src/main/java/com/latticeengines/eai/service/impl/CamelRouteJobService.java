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
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("camelRouteJobService")
public class CamelRouteJobService {

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    public ApplicationId submitImportJob(ImportConfiguration importConfig) {
        if (!ImportConfiguration.ImportType.CamelRoute.equals(importConfig.getImportType())) {
            throw new IllegalArgumentException("An import of type " + importConfig.getImportType()
                    + " was directed to " + this.getClass().getSimpleName());
        }

        EaiJob eaiJob = createJob(importConfig);
        ApplicationId appId = jobService.submitJob(eaiJob);
        eaiJob.setId(appId.toString());
        jobEntityMgr.create(eaiJob);

        return appId;
    }

    private EaiJob createJob(ImportConfiguration importConfig) {
        EaiJob eaiJob = new EaiJob();
        String customerSpace = CustomerSpace.parse(this.getClass().getSimpleName()).toString();

        eaiJob.setClient("eaiClient");
        eaiJob.setCustomer(customerSpace);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customerSpace);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getPropDataQueueNameForSubmission());

        Properties containerProperties = new Properties();
        containerProperties.put(ImportProperty.EAICONFIG, importConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "128");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        eaiJob.setAppMasterPropertiesObject(appMasterProperties);
        eaiJob.setContainerPropertiesObject(containerProperties);
        return eaiJob;
    }

}
