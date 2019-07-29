package com.latticeengines.apps.cdl.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.MigrateTrackingService;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.CDLEntityMatchMigrationWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("cdlEntityMatchMigrationWorkflowSubmitter")
public class CDLEntityMatchMigrationWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CDLEntityMatchMigrationWorkflowSubmitter.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private TenantService tenantService;

    @Inject
    private ActionService actionService;

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
        Map<BusinessEntity, Action> actionMap = registerEmptyAction(customerSpace, userId, pidWrapper.getPid());
        CDLEntityMatchMigrationWorkflowConfiguration configuration = generateConfiguration(customerSpace,
                migrateTracking.getPid(), dataFeedTaskMap, actionMap, userId);

        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private Map<BusinessEntity, Action> registerEmptyAction(CustomerSpace customerSpace, String userId,
                                                            Long workflowPid) {
        Map<BusinessEntity, Action> actionMap = new HashedMap<>();
        if (dataCollectionProxy.getTable(customerSpace.toString(), BusinessEntity.Account.getBatchStore()) != null) {
            Action action = getAction(customerSpace, userId, workflowPid);
            actionMap.put(BusinessEntity.Account, actionService.create(action));
        }
        if (dataCollectionProxy.getTable(customerSpace.toString(), BusinessEntity.Contact.getBatchStore()) != null) {
            Action action = getAction(customerSpace, userId, workflowPid);
            actionMap.put(BusinessEntity.Contact, actionService.create(action));
        }
        if (dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedRawTransaction) != null) {
            Action action = getAction(customerSpace, userId, workflowPid);
            actionMap.put(BusinessEntity.Transaction, actionService.create(action));
        }
        return actionMap;
    }

    private Action getAction(CustomerSpace customerSpace, String userId, Long workflowPid) {
        Action action = new Action();
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setTrackingPid(workflowPid);
        action.setActionInitiator(userId);
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        }
        action.setTenant(tenant);
        return action;
    }

    private CDLEntityMatchMigrationWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
                                                                               Long trackingPid,
                                                                               Map<BusinessEntity, List<String>> dataFeedTaskMap,
                                                                               Map<BusinessEntity, Action> actionMap,
                                                                               String userId) {
        CDLEntityMatchMigrationWorkflowConfiguration.Builder builder = new CDLEntityMatchMigrationWorkflowConfiguration.Builder();

        return builder.customer(customerSpace)
                .microServiceHostPort(microserviceHostPort)
                .internalResourceHostPort(internalResourceHostPort)
                .userId(userId)
                .migrateTrackingPid(trackingPid)
                .dataFeedTaskMap(dataFeedTaskMap)
                .actionMap(actionMap)
                .build();
    }
}
