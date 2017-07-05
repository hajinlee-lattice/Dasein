package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DLTenantMappingService;
import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.workflow.CDLDataFeedImportWorkflowSubmitter;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataFeedTaskManagerService")
public class DataFeedTaskManagerServiceImpl implements DataFeedTaskManagerService {

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private DLTenantMappingService dlTenantMappingService;

    @Autowired
    private CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter;

    @Value("${cdl.dataloader.tenant.mapping.enabled:false}")
    private boolean dlTenantMappingEnabled;

    @Override
    public String createDataFeedTask(String customerSpaceStr, String feedType, String entity, String source,
                                     String metadata) {
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(source);
        Table newMeta = dataFeedMetadataService.getMetadata(metadata);
        CustomerSpace customerSpace = dataFeedMetadataService.getCustomerSpace(metadata);
        if (dlTenantMappingEnabled) {
            customerSpace = mapCustomerSpace(customerSpace);
        }
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        MultiTenantContext.setTenant(tenant);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask != null) {
            Table originMeta = dataFeedTask.getImportTemplate();
            if (!dataFeedMetadataService.compareMetadata(originMeta, newMeta)) {
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);
                dataFeedTask.setImportTemplate(newMeta);
                dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            }
            return dataFeedTask.getUniqueId();
        } else {
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedTask.setImportTemplate(newMeta);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity);
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(source);
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            return dataFeedTask.getUniqueId();
        }
    }

    @Override
    public String submitImportJob(String customerSpaceStr, String taskIdentifier, String importConfig) {
        CustomerSpace customerSpace = CustomerSpace.parse(customerSpaceStr);
        if (dlTenantMappingEnabled) {
            customerSpace = mapCustomerSpace(customerSpace);
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), taskIdentifier);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the data feed task!");
        }
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(dataFeedTask.getSource());
        String connectorConfig = dataFeedMetadataService.getConnectorConfig(importConfig, dataFeedTask.getUniqueId());

        ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask, connectorConfig);
        return appId.toString();
    }

    private CustomerSpace mapCustomerSpace(CustomerSpace customerSpace) {
        CustomerSpace newCustomerSpace = customerSpace;
        String dlTenantId = customerSpace.getTenantId();
        DLTenantMapping dlTenantMapping = dlTenantMappingService.getDLTenantMapping(dlTenantId, "*");
        if (dlTenantMapping != null) {
            newCustomerSpace = CustomerSpace.parse(dlTenantMapping.getTenantId());
        }
        return newCustomerSpace;
    }
}
