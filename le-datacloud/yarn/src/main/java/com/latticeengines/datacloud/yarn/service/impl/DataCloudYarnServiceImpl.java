package com.latticeengines.datacloud.yarn.service.impl;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.DataCloudProperty;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("propDataYarnService")
public class DataCloudYarnServiceImpl implements DataCloudYarnService {

    @Autowired
    @Qualifier(value = "jobEntityMgr")
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Autowired
    private ApplicationContext applicationContext;

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
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "2048");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "2");

        propDataJob.setAppMasterPropertiesObject(appMasterProperties);
        propDataJob.setContainerPropertiesObject(containerProperties);
        return propDataJob;
    }

}
