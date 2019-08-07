package com.latticeengines.cdl.workflow.listeners;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("cdlEntityMatchMigrationListener")
public class CDLEntityMatchMigrationListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(CDLEntityMatchMigrationListener.class);

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String customerSpace = job.getTenant().getId();
        Long trackingPid =
                Long.parseLong(job.getInputContextValue(WorkflowContextConstants.Inputs.IMPORT_MIGRATE_TRACKING_ID));
        if (jobExecution.getStatus().isUnsuccessful()) {
            List<Long> actionIds = migrateTrackingProxy.getRegisteredActions(customerSpace, trackingPid);
            if (actionIds != null) {
                actionIds = actionIds.stream().filter(Objects::nonNull).collect(Collectors.toList());
            }
            if (CollectionUtils.isNotEmpty(actionIds)) {
                actionIds.forEach(actionId -> actionProxy.cancelAction(customerSpace, actionId));
            }
            migrateTrackingProxy.updateStatus(customerSpace, trackingPid, ImportMigrateTracking.Status.FAILED);
        } else if (BatchStatus.COMPLETED.equals(jobExecution.getStatus())) {
            migrateTrackingProxy.updateStatus(customerSpace, trackingPid, ImportMigrateTracking.Status.COMPLETED);
//            metadataProxy.updateImportTracking(customerSpace, trackingPid);
            //TODO: remove old datafeed tasks.
        } else {
            log.warn(String.format("CDLEntityMatchMigration job ends in unknown status: %s",
                    jobExecution.getStatus().name()));
        }
    }
}
