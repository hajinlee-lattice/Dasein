package com.latticeengines.apps.cdl.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.MigrateTrackingService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.CDLEntityMatchMigrationWorkflowConfiguration;

@Component("cdlEntityMatchMigrationWorkflowSubmitter")
public class CDLEntityMatchMigrationWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CDLEntityMatchMigrationWorkflowSubmitter.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private MigrateTrackingService migrateTrackingService;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, String userId, WorkflowPidWrapper pidWrapper) {
        log.info(String.format("Start submit EntityMatchMigrate job for tenant %s, by user %s",
                customerSpace.getTenantId(), userId));
        MigrateTracking migrateTracking = migrateTrackingService.create(customerSpace.toString());
        Map<BusinessEntity, List<String>> dataFeedTaskMap = new HashMap<>();
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace.toString());
        if (dataFeed == null || CollectionUtils.isEmpty(dataFeed.getTasks())) {
            throw new RuntimeException("There is no template to be migrated!");
        }
        for (DataFeedTask dataFeedTask : dataFeed.getTasks()) {
            BusinessEntity entity = BusinessEntity.getByName(dataFeedTask.getEntity());
            dataFeedTaskMap.putIfAbsent(entity, new ArrayList<>());
            dataFeedTaskMap.get(entity).add(dataFeedTask.getUniqueId());
        }
        CDLEntityMatchMigrationWorkflowConfiguration configuration = generateConfiguration(customerSpace,
                migrateTracking.getPid(), dataFeedTaskMap, userId);

        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private CDLEntityMatchMigrationWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
                                                                               Long trackingPid,
                                                                               Map<BusinessEntity, List<String>> dataFeedTaskMap,
                                                                               String userId) {
        CDLEntityMatchMigrationWorkflowConfiguration.Builder builder = new CDLEntityMatchMigrationWorkflowConfiguration.Builder();

        return builder.customer(customerSpace)
                .microServiceHostPort(microserviceHostPort)
                .internalResourceHostPort(internalResourceHostPort)
                .userId(userId)
                .migrateTrackingPid(trackingPid)
                .dataFeedTaskMap(dataFeedTaskMap)
                .build();
    }
}
