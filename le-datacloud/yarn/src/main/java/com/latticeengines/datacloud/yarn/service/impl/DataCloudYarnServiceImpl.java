package com.latticeengines.datacloud.yarn.service.impl;

import java.util.Properties;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import com.latticeengines.common.exposed.util.JacocoUtils;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.DataCloudProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("propDataYarnService")
public class DataCloudYarnServiceImpl implements DataCloudYarnService {

    @Resource(name = "jobEntityMgr")
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Autowired
    private ApplicationContext applicationContext;

    @Value("${datacloud.yarn.container.mem.mb}")
    private int yarnContainerMemory;

    @Value("${datacloud.yarn.container.vcores}")
    private int yarnContainerVCores;

    @Value("${datacloud.yarn.container.mem.mb.actors}")
    private int yarnContainerMemoryActors;

    @Value("${datacloud.yarn.container.vcores.actors}")
    private int yarnContainerVCoresActors;

    @Value("${dataplatform.trustore.jks}")
    private String trustStoreJks;

    @Override
    public ApplicationId submitPropDataJob(DataCloudJobConfiguration jobConfiguration) {
        Job propDataJob = createJob(jobConfiguration);
        ApplicationId appId = jobService.submitJob(propDataJob);
        propDataJob.setId(appId.toString());

        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        final Job job = propDataJob;
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                jobEntityMgr.create(job);
            }
        });

        return appId;
    }

    private Job createJob(DataCloudJobConfiguration jobConfiguration) {
        Job propDataJob = new Job();

        String customer = jobConfiguration.getCustomerSpace().toString();
        propDataJob.setClient("datacloudClient");
        propDataJob.setCustomer(customer);

        String queueName = jobConfiguration.getYarnQueue();
        if (StringUtils.isEmpty(queueName)) {
            queueName = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        }

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), queueName);
        if (StringUtils.isNotEmpty(jobConfiguration.getAppName())) {
            appMasterProperties.put(AppMasterProperty.APP_NAME.name(), jobConfiguration.getAppName());
        }

        Properties containerProperties = new Properties();
        containerProperties.put(DataCloudProperty.DATACLOUD_CONFIG, jobConfiguration.toString());

        if (MatchUtils.isValidForAccountMasterBasedMatch(jobConfiguration.getMatchInput().getDataCloudVersion())) {
            appMasterProperties.put(AppMasterProperty.MEMORY.name(), String.valueOf(yarnContainerMemoryActors));
            appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), String.valueOf(yarnContainerVCoresActors));
            containerProperties.put(ContainerProperty.MEMORY.name(), String.valueOf(yarnContainerMemoryActors));
            containerProperties.put(ContainerProperty.VIRTUALCORES.name(), String.valueOf(yarnContainerVCoresActors));
        } else {
            appMasterProperties.put(AppMasterProperty.MEMORY.name(), String.valueOf(yarnContainerMemory));
            appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), String.valueOf(yarnContainerVCores));
            containerProperties.put(ContainerProperty.MEMORY.name(), String.valueOf(yarnContainerMemory));
            containerProperties.put(ContainerProperty.VIRTUALCORES.name(), String.valueOf(yarnContainerVCores));
        }

        containerProperties.put(ContainerProperty.PRIORITY.name(), "2");

        JacocoUtils.setJacoco(containerProperties, "datacloud");

        if (StringUtils.isNotBlank(trustStoreJks)) {
            containerProperties.put(ContainerProperty.TRUST_STORE.name(), trustStoreJks);
        }

        propDataJob.setAppMasterPropertiesObject(appMasterProperties);
        propDataJob.setContainerPropertiesObject(containerProperties);
        return propDataJob;
    }

}
