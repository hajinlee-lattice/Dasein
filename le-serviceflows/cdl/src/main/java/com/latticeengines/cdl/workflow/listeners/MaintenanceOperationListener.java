package com.latticeengines.cdl.workflow.listeners;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("maintenanceOperationListener")
public class MaintenanceOperationListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceOperationListener.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private PlsInternalProxy plsInternalProxy;

    @Inject
    private ActionProxy actionProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String customerSpace = job.getTenant().getId();
        String status = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_STATUS);
        String executionId = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_EXECUTION_ID);
        DataFeedExecution dataFeedExecution = null;
        if (StringUtils.isEmpty(status)) {
            throw new RuntimeException("Cannot get initial data feed status for customer: " + customerSpace);
        }
        if (jobExecution.getStatus().isUnsuccessful()) {
            // reset data feed status.
            log.info(String.format("Maintenance workflow failed. Update datafeed status for customer %s with status %s",
                    customerSpace, status));
            if (StringUtils.isEmpty(executionId))
                dataFeedExecution = dataFeedProxy.failExecution(customerSpace, status);
            else
                dataFeedExecution = dataFeedProxy.failExecution(customerSpace, status, Long.valueOf(executionId));
            if (dataFeedExecution.getStatus() != DataFeedExecution.Status.Failed) {
                throw new RuntimeException("Cannot fail execution!");
            }

        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            // check data feed status? should be Initing or InitialLoaded?
            String reportName = job.getReportName(ReportPurpose.MAINTENANCE_OPERATION_SUMMARY.getKey());
            if (!StringUtils.isEmpty(reportName)) {
                Report report = plsInternalProxy.findReportByName(reportName, customerSpace);
                updateMaintenanceActionConfiguration(job, report);
            }
            if (StringUtils.isEmpty(executionId))
                dataFeedExecution = dataFeedProxy.finishExecution(customerSpace, status);
            else
                dataFeedExecution = dataFeedProxy.finishExecution(customerSpace, status, Long.valueOf(executionId));
            if (dataFeedExecution.getStatus() != DataFeedExecution.Status.Completed) {
                throw new RuntimeException("Cannot finish execution!");
            }
        } else {
            log.error(String.format("CDL maintenance job ends in unknown status: %s", jobExecution.getStatus().name()));
        }

        dataFeedProxy.updateDataFeedMaintenanceMode(customerSpace, false);
    }

    private void updateMaintenanceActionConfiguration(WorkflowJob job, Report report) {
        if (report == null) {
            return;
        }
        String impactedEntities = job
                .getOutputContextValue(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES);
        if (StringUtils.isEmpty(impactedEntities)) {
            log.warn("Impacted Entities is Empty!");
            return;
        }
        List<?> entityList = JsonUtils.deserialize(impactedEntities, List.class);
        List<BusinessEntity> entities = JsonUtils.convertList(entityList, BusinessEntity.class);
        String ActionPidStr = job.getInputContextValue(WorkflowContextConstants.Inputs.ACTION_ID);
        if (ActionPidStr != null) {
            Long pid = Long.parseLong(ActionPidStr);
            log.info(String.format("Updating an actionPid=%d for job=%d", pid, job.getWorkflowId()));
            Action action = actionProxy.getActionsByPids(job.getTenant().getId(), Collections.singletonList(pid))
                    .get(0);
            CleanupActionConfiguration config = action.getActionConfiguration() == null
                    ? new CleanupActionConfiguration() : (CleanupActionConfiguration) action.getActionConfiguration();
            try {
                ObjectMapper om = JsonUtils.getObjectMapper();
                ObjectNode jsonReport = (ObjectNode) om.readTree(report.getJson().getPayload());
                entities.forEach(entity -> {
                    if (jsonReport.has(entity.name() + "_Deleted")) {
                        Long delete = Long.valueOf(jsonReport.get(entity.name() + "_Deleted").toString());
                        config.addDeletedRecords(entity, delete);
                    }
                });
            } catch (Exception e) {
                log.error("Cannot get delete count for report: " + report.getName());
            }
            action.setActionConfiguration(config);
            actionProxy.updateAction(job.getTenant().getId(), action);
        } else {
            log.warn(String.format("ActionPid is not correctly registered by workflow job=%d", job.getWorkflowId()));
        }
    }
}
