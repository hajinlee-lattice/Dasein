package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityMetricsService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.cdl.workflow.ProcessAnalyzeWorkflowSubmitter;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

import reactor.core.publisher.Flux;

@Component("proxyResourceService")
public class ProxyResourceServiceImpl implements ProxyResourceService {
    private static Logger log = LoggerFactory.getLogger(ProxyResourceServiceImpl.class);

    @Value("${cdl.processAnalyze.retry.expired.time}")
    private long retryExpiredTime;

    @Inject
    private ActionService actionService;

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ActivityMetricsService activityMetricsService;

    @Inject
    private DataFeedService datafeedService;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private DataFeedExecutionEntityMgr dataFeedExecutionEntityMgr;

    @Inject
    private ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter;

    @Override
    public List<ActivityMetrics> getActivityMetrics() {
        List<ActivityMetrics> metrics = activityMetricsService.findWithType(ActivityType.PurchaseHistory);
        if (metrics == null) {
            return Collections.emptyList();
        }
        return metrics;
    }

    @Override
    public void registerAction(Action action, String user) {
        if (action != null) {
            action.setTenant(MultiTenantContext.getTenant());
            action.setActionInitiator(user);
            log.info(String.format("Registering action %s", action));
            ActionConfiguration actionConfig = action.getActionConfiguration();
            if (actionConfig != null) {
                action.setDescription(actionConfig.serialize());
            }
            actionService.create(action);
        }
    }

    @Override
    public Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity,
                                                    DataCollection.Version version) {
        Flux<ColumnMetadata> flux = servingStoreService.getDecoratedMetadata(customerSpace, entity, version,
                Collections.singletonList(ColumnSelection.Predefined.Model));
        flux = flux.map(cm -> {
            cm.setApprovedUsageList(Collections.singletonList(ApprovedUsage.MODEL_ALLINSIGHTS));
            if (cm.getTagList() == null || (cm.getTagList() != null && !cm.getTagList().contains(Tag.EXTERNAL))) {
                cm.setTagList(Collections.singletonList(Tag.INTERNAL));
            }
            return cm;
        });
        return flux;
    }

    @Override
    public Table getTable(String customerSpace, TableRoleInCollection role, DataCollection.Version version) {
        List<Table> tables = dataCollectionService.getTables(customerSpace, null, role, version);
        if (tables == null || tables.isEmpty()) {
            return null;
        } else {
            return tables.get(0);
        }
    }

    @Override
    public Table getTable(String customerSpace, TableRoleInCollection role) {
        return getTable(customerSpace, role, null);
    }

    @Override
    public DataCollection getDataCollection(String customerSpace) {
        return dataCollectionService.getDataCollection(customerSpace, null);
    }

    @Override
    public DataCollectionArtifact createArtifact(String customerSpace, DataCollectionArtifact artifact, DataCollection.Version version) {
        return dataCollectionService.createArtifact(customerSpace, artifact.getName(), artifact.getUrl(),
                artifact.getStatus(), version);
    }

    @Override
    public void resetTable(String customerSpace, TableRoleInCollection tableRole) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataCollectionService.resetTable(customerSpace, null, tableRole);
    }

    @Override
    public DataCollectionStatus getOrCreateDataCollectionStatus(String customerSpace, DataCollection.Version version) {
        return dataCollectionService.getOrCreateDataCollectionStatus(customerSpace, version);
    }

    @Override
    public DataCollection.Version getActiveVersion(String customerSpace) {
        return getDataCollection(customerSpace).getVersion();
    }

    @Override
    public boolean hasContact(String customerSpace, DataCollection.Version version) {
        DataCollectionStatus status = getOrCreateDataCollectionStatus(customerSpace, version);
        Long count = status.getContactCount();
        return count != null && count > 0;
    }

    @Override
    public void resetImport(String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.resetImport(customerSpace, "");
    }

    @Override
    public DataFeed getDataFeed(String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.getOrCreateDataFeed(customerSpace);
    }

    @Override
    public DataFeedExecution failExecution(String customerSpace, String initialDataFeedStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.failExecution(customerSpace, "", initialDataFeedStatus);
    }

    @Override
    public Long lockExecution(String customerSpace, DataFeedExecutionJobType jobType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.lockExecution(customerSpace, "", jobType);
    }

    @Override
    public DataFeedExecution getLatestExecution(String customerSpace, DataFeedExecutionJobType jobType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        DataFeed dataFeed = datafeedService.getDefaultDataFeed(customerSpace);
        return datafeedService.getLatestExecution(customerSpace, dataFeed.getName(), jobType);
    }

    @Override
    public Long restartExecution(String customerSpace, DataFeedExecutionJobType jobType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.restartExecution(customerSpace, "", jobType);
    }

    @Override
    public void updateDataFeedStatus(String customerSpace, String status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeed(customerSpace, "", status);
    }

    @Override
    public DataFeedExecution finishExecution(String customerSpace, String initialDataFeedStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.finishExecution(customerSpace, "", initialDataFeedStatus);
    }

    @Override
    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType, entity);
    }

    @Override
    public void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.createDataFeedTask(customerSpace, dataFeedTask);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String taskId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, taskId);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType);
    }

    @Override
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String customerSpace, String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskWithSameEntity(customerSpace, entity);
    }

    @Override
    public ApplicationId restart(String customerSpace, Integer memory, Boolean autoRetry, Boolean skipMigrationTrack) {
        customerSpace = MultiTenantContext.getCustomerSpace().toString();
        checkRetry(customerSpace);
        return processAnalyzeWorkflowSubmitter.retryLatestFailed(customerSpace, memory, autoRetry,
                skipMigrationTrack);
    }

    private void checkRetry(String customerSpace) {
        DataFeed dataFeed = datafeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed == null) {
            String errorMessage = String.format(
                    "we can't restart processAnalyze workflow for %s, dataFeed is empty.", customerSpace);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        DataFeedExecution execution;
        try {
            execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(dataFeed,
                    DataFeedExecutionJobType.PA);
        } catch (Exception e) {
            execution = null;
        }
        if (execution == null) {
            String errorMessage = String.format("we can't restart processAnalyze workflow for %s, dataFeedExecution " +
                            "is empty."
                    , customerSpace);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        if (!DataFeedExecution.Status.Failed.equals(execution.getStatus())) {
            String errorMessage = String.format("we can't restart processAnalyze workflow for %s, last PA isn't fail. "
                    , customerSpace);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        long currentTime = new Date().getTime();
        if (execution.getUpdated() == null || (execution.getUpdated().getTime() - (currentTime - retryExpiredTime * 1000) < 0)) {
            String errorMessage = String.format("we can't restart processAnalyze workflow for %s, last PA has been " +
                            "more than %d second. "
                    , customerSpace, retryExpiredTime);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    @Override
    public ApplicationId processAnalyze(ProcessAnalyzeRequest request) {
        return processAnalyzeWorkflowSubmitter.submit(MultiTenantContext.getCustomerSpace().toString(),
                request == null ? new ProcessAnalyzeRequest() : request, new WorkflowPidWrapper(-1L));
    }
}
