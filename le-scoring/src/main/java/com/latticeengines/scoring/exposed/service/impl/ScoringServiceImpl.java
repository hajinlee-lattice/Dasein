package com.latticeengines.scoring.exposed.service.impl;

import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringProperty;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.scoring.exposed.service.ScoringService;

@Component("scoringService")
public class ScoringServiceImpl implements ScoringService {

    private static final String SCORING_CLIENT = "scoringClient";

    @Autowired
    private JobService jobService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Override
    public ApplicationId submitScoreWorkflow(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        Job job = createJob(rtsBulkScoringConfig);
        ApplicationId appId = jobService.submitJob(job);
        job.setId(appId.toString());
        jobEntityMgr.create(job);
        return appId;
    }

    private Job createJob(RTSBulkScoringConfiguration rtsBulkScoringConfig) {

        Job job = new Job();
        String customerSpace = rtsBulkScoringConfig.getCustomerSpace().toString();

        job.setClient(SCORING_CLIENT);
        job.setCustomer(customerSpace);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(),
                String.format("customer%s", String.valueOf(System.currentTimeMillis())));
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getScoringQueueNameForSubmission());

        Properties containerProperties = new Properties();
        containerProperties.put(RTSBulkScoringProperty.RTS_BULK_SCORING_CONFIG, rtsBulkScoringConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "1096");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        job.setAppMasterPropertiesObject(appMasterProperties);
        job.setContainerPropertiesObject(containerProperties);

        return job;
    }
}
