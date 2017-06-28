package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedMetadataService;
import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.workflow.CDLDataFeedImportWorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("dataFeedTaskManagerService")
public class DataFeedTaskManagerServiceImpl implements DataFeedTaskManagerService {

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter;

    @Override
    public String createDataFeedTask(String feedType, String entity, String source, String metadata) {
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(source);
        Table newMeta = dataFeedMetadataService.getMetadata(metadata);
        CustomerSpace customerSpace = dataFeedMetadataService.getCustomerSpace(metadata);
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity);
        if (dataFeedTask != null) {
            Table originMeta = dataFeedTask.getImportTemplate();
            if (!dataFeedMetadataService.compareMetadata(originMeta, newMeta)) {
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);
                //metadataProxy.createTable(customerSpace.toString(), newMeta.getName(), newMeta);
                dataFeedTask.setImportTemplate(newMeta);
                dataFeedProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            }
            DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
            return getIdentifierFromDataFeedTask(dataFeed.getName(), dataFeedTask);
        } else {
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setImportTemplate(newMeta);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity);
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(source);
            //dataFeedTask.setImportData(TableUtils.clone(newMeta, newMeta.getName()));
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            //todo
            //dataFeedTask.setStagingDir("");
            //dataFeedTask.setDataFeed(getDataFeed(customerSpace.toString(), datafeedName));
            //metadataProxy.createTable(customerSpace.toString(), newMeta.getName(), newMeta);
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
            return getIdentifierFromDataFeedTask(dataFeed.getName(), dataFeedTask);
        }
    }

    @Override
    public String submitImportJob(String taskIdentifier, String importConfig) {
        String[] keys = getTaskKeyFromIdentifier(taskIdentifier);
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(keys[0]);
        CustomerSpace customerSpace = dataFeedMetadataService.getCustomerSpace(importConfig);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), keys[0], keys[2],
                keys[1]);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the data feed task!");
        }
        String connectorConfig = dataFeedMetadataService.getConnectorConfig(importConfig, dataFeedTask.getPid().toString());

        ApplicationId appId = cdlDataFeedImportWorkflowSubmitter.submit(customerSpace, dataFeedTask, connectorConfig);
        return appId.toString();
    }

    private String getIdentifierFromDataFeedTask(String dataFeedName, DataFeedTask dataFeedTask) {
        return String.format("%s.%s.%s.%s", dataFeedTask.getSource(), dataFeedTask.getEntity(), dataFeedTask
                .getFeedType(), dataFeedName);
    }

    private String[] getTaskKeyFromIdentifier(String identifier) {
        String[] parts = identifier.split("\\.");
        if (parts.length != 4) {
            throw new RuntimeException(
                    String.format("Identifiers must in a 4-part format.  Identifier %s is invalid!", identifier));
        }
        return parts;
    }
}
