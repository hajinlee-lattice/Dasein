package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class CDLDataFeedImportWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CDLDataFeedImportWorkflowSubmitter.class);

    @Inject
    private TenantService tenantService;

    @Inject
    private ActionService actionService;

    @Inject
    private WorkflowProxy workflowProxy;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, DataFeedTask dataFeedTask, String connectorConfig,
                                CSVImportFileInfo csvImportFileInfo, boolean s3ImportEmail,
                                S3ImportEmailInfo emailInfo, WorkflowPidWrapper pidWrapper) {
        log.info(String.format("CDLDataFeedImport WorkflowJob created for customer=%s with pid=%s", customerSpace,
                pidWrapper.getPid()));
        Action action = registerAction(customerSpace, dataFeedTask, csvImportFileInfo, pidWrapper.getPid());
        CDLDataFeedImportWorkflowConfiguration configuration = generateConfiguration(customerSpace, dataFeedTask,
                connectorConfig, csvImportFileInfo, action.getPid(), s3ImportEmail, emailInfo);

        ApplicationId appId = workflowJobService.submit(configuration, pidWrapper.getPid());
        return appId;
    }

    @VisibleForTesting
    Action registerAction(CustomerSpace customerSpace, DataFeedTask dataFeedTask, CSVImportFileInfo csvImportFileInfo,
            Long workflowPid) {
        log.info(String.format("Registering an import action for datafeedTask=%s, tenant=%s",
                dataFeedTask.getUniqueId(), customerSpace.toString()));
        Action action = new Action();
        ImportActionConfiguration config = new ImportActionConfiguration();
        config.setDataFeedTaskId(dataFeedTask.getUniqueId());
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setTrackingPid(workflowPid);
        action.setActionInitiator(csvImportFileInfo.getFileUploadInitiator());
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        }
        action.setTenant(tenant);
        action.setActionConfiguration(config);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid: " + tenant);
        }
        log.info(String.format("Action=%s", action));
        return actionService.create(action);
    }

    public void registerFailedAction(String customerSpace, String taskId, String actionInitiator,
            S3FileToHdfsConfiguration importConfig, ErrorDetails errorDetails, String initialS3FilePath) {
        log.info(String.format("Registering an import action for datafeedTask=%s, tenant=%s",
                taskId, customerSpace));
        Action action = new Action();
        ImportActionConfiguration config = new ImportActionConfiguration();
        config.setDataFeedTaskId(taskId);
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        Job failedJob = new Job();
        failedJob.setJobType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.getName());
        failedJob.setUser(actionInitiator);
        Map<String, String> inputs = new HashMap<>();
        inputs.put(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME, importConfig.getS3FileName());
        inputs.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, importConfig.getS3FileName());
        inputs.put(WorkflowContextConstants.Inputs.SOURCE_FILE_PATH, initialS3FilePath);
        failedJob.setInputs(inputs);
        failedJob.setErrorCode(errorDetails.getErrorCode());
        failedJob.setErrorMsg(errorDetails.getErrorMsg());
        Long failedWorkflowId = workflowProxy.createFailedWorkflowJob(customerSpace, failedJob);
        action.setTrackingPid(failedWorkflowId);
        action.setActionInitiator(actionInitiator);
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace));
        }
        action.setTenant(tenant);
        action.setActionConfiguration(config);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid: " + tenant);
        }
        actionService.create(action);
    }

    private CDLDataFeedImportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
            DataFeedTask dataFeedTask, String connectorConfig, CSVImportFileInfo csvImportFileInfo,
            @NonNull Long actionPid, boolean s3ImportEmail, S3ImportEmailInfo emailInfo) {
        String filePath = "";
        if (StringUtils.isNotEmpty(csvImportFileInfo.getReportFilePath())) {
                filePath = csvImportFileInfo.getReportFilePath();
        }

        String emailInfoStr = "";
        if (s3ImportEmail && emailInfo != null) {
            emailInfoStr = JsonUtils.serialize(emailInfo);
        }
        BusinessEntity entity = BusinessEntity.getByName(dataFeedTask.getEntity());
        return new CDLDataFeedImportWorkflowConfiguration.Builder() //
                .customer(customerSpace) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .dataFeedTaskId(dataFeedTask.getUniqueId()) //
                .fileValidation(entity) //
                .importConfig(connectorConfig) //
                .userId(csvImportFileInfo.getFileUploadInitiator()) //
                .inputProperties(ImmutableMap.<String, String>builder()
                        .put(WorkflowContextConstants.Inputs.DATAFEEDTASK_IMPORT_IDENTIFIER, dataFeedTask.getUniqueId()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME, csvImportFileInfo.getReportFileName()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME,
                                csvImportFileInfo.getReportFileDisplayName()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_FILE_PATH, filePath)
                        .put(WorkflowContextConstants.Inputs.ACTION_ID, actionPid.toString()) //
                        .put(WorkflowContextConstants.Inputs.S3_IMPORT_EMAIL_FLAG, String.valueOf(s3ImportEmail))//
                        .put(WorkflowContextConstants.Inputs.S3_IMPORT_EMAIL_INFO, emailInfoStr)
                        .build())
                .build();
    }
}
