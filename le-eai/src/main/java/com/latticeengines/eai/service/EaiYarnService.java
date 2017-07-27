package com.latticeengines.eai.service;

import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.common.exposed.util.JacocoUtils;
import com.latticeengines.domain.exposed.BaseContext;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;

public interface EaiYarnService {

    default EaiJob createJob(EaiJobConfiguration eaiJobConfig) {
        EaiJob eaiJob = new EaiJob();
        StringBuilder customerSpace = new StringBuilder();
        if (eaiJobConfig.getCustomerSpace() != null) {
            customerSpace.append(eaiJobConfig.getCustomerSpace().getTenantId()).append('~');
        }
        customerSpace.append(eaiJobConfig.getClass().getSimpleName().replace("Configuration", ""));
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

        JacocoUtils.setJacoco(containerProperties, "eai");

        eaiJob.setAppMasterPropertiesObject(appMasterProperties);
        eaiJob.setContainerPropertiesObject(containerProperties);
        return eaiJob;
    }

    void submitSingleYarnContainerJob(EaiJobConfiguration eaiJobConfig, BaseContext context);

    ApplicationId submitMRJob(String mrJobName, Properties props);

    ApplicationId submitSingleYarnContainerJob(EaiJobConfiguration eaiJobConfig);

}
