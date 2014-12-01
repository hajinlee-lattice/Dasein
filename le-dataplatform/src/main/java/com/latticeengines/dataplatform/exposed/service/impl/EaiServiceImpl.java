package com.latticeengines.dataplatform.exposed.service.impl;

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.exposed.service.EaiService;
import com.latticeengines.dataplatform.runtime.eai.EaiContainerProperty;
import com.latticeengines.dataplatform.service.eai.EaiJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.EaiConfiguration;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

@Component("eaiService")
public class EaiServiceImpl implements EaiService {

    private static final Log log = LogFactory.getLog(EaiServiceImpl.class);

    @Autowired
    private EaiJobService eaiJobService;

    @Override
    public ApplicationId invokeEai(EaiConfiguration eaiConfig) {
        return eaiJobService.submitJob(createJob(eaiConfig));
    }

    private EaiJob createJob(EaiConfiguration eaiConfig) {
        EaiJob eaiJob = new EaiJob();
        String customer = eaiConfig.getCustomer();
        String targetPath = eaiConfig.getTargetPath();
        List<Table> tables = eaiConfig.getTables();

        eaiJob.setClient("eaiClient");
        eaiJob.setCustomer(customer);
        eaiJob.setTargetPath(targetPath);
        StringBuffer sb = new StringBuffer();
        for (Table table : tables) {
            sb.append(table.getName() + " ");
        }
        eaiJob.setTables(sb.toString().trim());

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getNonMRQueueNameForSubmission(0));

        Properties containerProperties = new Properties();
        containerProperties.put(EaiContainerProperty.TABLES.name(), tables.toString());
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
        return eaiJobService.getJobStatus(applicationId);
    }

}
