package com.latticeengines.apps.cdl.workflow;

import java.util.Date;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.ConvertBatchStoreInfoService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.ConvertBatchStoreToImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreToImportServiceConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("convertBatchStoreToImportWorkflowSubmitter")
public class ConvertBatchStoreToImportWorkflowSubmitter extends WorkflowSubmitter {

    @Inject
    private TenantService tenantService;

    @Inject
    private DataFeedTaskService dataFeedTaskService;

    @Inject
    private ConvertBatchStoreInfoService convertBatchStoreInfoService;

    @Inject
    private ActionService actionService;

    @Inject
    private MetadataProxy metadataProxy;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, String userId,
                                BusinessEntity entity, String templateName,
                                String feedType, String subType,
                                Map<String, String> renameMap,
                                Map<String, String> duplicateMap, WorkflowPidWrapper pidWrapper) {
        Action action = createImportAction(customerSpace, userId, pidWrapper.getPid());
        Table template = metadataProxy.getImportTable(customerSpace.toString(), templateName);
        if (template == null || CollectionUtils.isEmpty(template.getAttributes())) {
            throw new RuntimeException("Cannot convert batch store to empty template!");
        }
        DataFeedTask dataFeedTask = createDataFeedTask(customerSpace, entity, feedType, subType, template);

        ConvertBatchStoreInfo convertInfo = convertBatchStoreInfoService.create(customerSpace.toString());
        ConvertBatchStoreDetail detail = new ConvertBatchStoreDetail();
        detail.setTaskUniqueId(dataFeedTask.getUniqueId());
        detail.setRenameMap(renameMap);
        detail.setDuplicateMap(duplicateMap);
        convertBatchStoreInfoService.updateDetails(customerSpace.toString(), convertInfo.getPid(), detail);

        ConvertBatchStoreToImportServiceConfiguration serviceConfiguration = new ConvertBatchStoreToImportServiceConfiguration();
        serviceConfiguration.setEntity(entity);
        serviceConfiguration.setConvertInfoPid(convertInfo.getPid());
        ConvertBatchStoreToImportWorkflowConfiguration configuration = generateConfiguration(customerSpace, userId,
                action.getPid(), serviceConfiguration, entity);
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private Action createImportAction(CustomerSpace customerSpace, String userId, Long workflowPid) {
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
        return actionService.create(action);
    }

    private DataFeedTask createDataFeedTask(CustomerSpace customerSpace, BusinessEntity entity, String feedType,
                                            String subType, Table template) {
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        dataFeedTask.setImportTemplate(template);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setEntity(entity.name());
        dataFeedTask.setFeedType(feedType);
        dataFeedTask.setSource("File"); // could it be other source?
        dataFeedTask.setActiveJob("Not specified");
        dataFeedTask.setSourceConfig("Not specified");
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date(0L));
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setSubType(subType);
        dataFeedTaskService.createDataFeedTask(customerSpace.toString(), dataFeedTask);
        return dataFeedTask;
    }


    private ConvertBatchStoreToImportWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
                                                                                 String userId, Long actionPid,
                                                                                 ConvertBatchStoreToImportServiceConfiguration serviceConfiguration,
                                                                                 BusinessEntity entity) {

        ConvertBatchStoreToImportWorkflowConfiguration.Builder builder = new ConvertBatchStoreToImportWorkflowConfiguration.Builder();

        return builder.customer(customerSpace)
                .microServiceHostPort(microserviceHostPort)
                .internalResourceHostPort(internalResourceHostPort)
                .userId(userId)
                .actionPid(actionPid)
                .entity(entity)
                .convertBatchStoreServiceConfiguration(serviceConfiguration)
                .build();
    }
}
