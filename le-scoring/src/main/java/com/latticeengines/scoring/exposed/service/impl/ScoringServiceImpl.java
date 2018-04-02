package com.latticeengines.scoring.exposed.service.impl;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringProperty;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.scoring.exposed.service.ScoringService;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("scoringService")
public class ScoringServiceImpl implements ScoringService {

    private static final String SCORING_CLIENT = "scoringClient";

    @Autowired
    private JobService jobService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Value("${dataplatform.trustore.jks}")
    private String trustStoreJks;

    @Value("${scoring.processor.vcores}")
    private int vcores;

    @Value("${scoring.processor.memory}")
    private int memory;

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
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customerSpace);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(),
                LedpQueueAssigner.getRtsBulkScoringQueueNameForSubmission());

        Properties containerProperties = new Properties();
        containerProperties.put(RTSBulkScoringProperty.RTS_BULK_SCORING_CONFIG, rtsBulkScoringConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), vcores);
        containerProperties.put(ContainerProperty.MEMORY.name(), memory);
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        if (StringUtils.isNotBlank(trustStoreJks)) {
            containerProperties.put(ContainerProperty.TRUST_STORE.name(), trustStoreJks);
        }

        job.setAppMasterPropertiesObject(appMasterProperties);
        job.setContainerPropertiesObject(containerProperties);

        return job;
    }
}
