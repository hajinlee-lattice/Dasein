package com.latticeengines.apps.cdl.workflow;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class CDLDataFeedImportWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CDLDataFeedImportWorkflowSubmitter.class);

    @Inject
    private TenantService tenantService;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceProxy) {
        this.internalResourceProxy = internalResourceProxy;
    }

    public ApplicationId submit(CustomerSpace customerSpace, DataFeedTask dataFeedTask, String connectorConfig,
            CSVImportFileInfo csvImportFileInfo) {
        Action action = registerAction(customerSpace, dataFeedTask, csvImportFileInfo);
        CDLDataFeedImportWorkflowConfiguration configuration = generateConfiguration(customerSpace, dataFeedTask,
                connectorConfig, csvImportFileInfo, action.getPid());

        ApplicationId appId = workflowJobService.submit(configuration);
        return appId;
    }

    @VisibleForTesting
    Action registerAction(CustomerSpace customerSpace, DataFeedTask dataFeedTask, CSVImportFileInfo csvImportFileInfo) {
        log.info(String.format("Registering an action for datafeedTask=%s", dataFeedTask.getUniqueId()));
        Action action = new Action();
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setActionInitiator(csvImportFileInfo.getFileUploadInitiator());
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        }
        action.setTenant(tenant);
        log.info(String.format("Action=%s", action));
        return internalResourceProxy.createAction(tenant.getId(), action);
    }

    private CDLDataFeedImportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
            DataFeedTask dataFeedTask, String connectorConfig, CSVImportFileInfo csvImportFileInfo, Long actionPid) {

        return new CDLDataFeedImportWorkflowConfiguration.Builder() //
                .customer(customerSpace) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .dataFeedTaskId(dataFeedTask.getUniqueId()) //
                .importConfig(connectorConfig) //
                .inputProperties(ImmutableMap.<String, String> builder()
                        .put(WorkflowContextConstants.Inputs.DATAFEEDTASK_IMPORT_IDENTIFIER, dataFeedTask.getUniqueId()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME, csvImportFileInfo.getReportFileName()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME,
                                csvImportFileInfo.getReportFileDisplayName()) //
                        .put(WorkflowContextConstants.Inputs.ACTION_ID, actionPid.toString()) //
                        .build())
                .build();
    }
}
