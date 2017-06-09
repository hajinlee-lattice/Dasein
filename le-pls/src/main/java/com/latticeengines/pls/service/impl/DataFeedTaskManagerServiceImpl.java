package com.latticeengines.pls.service.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.DataFeedMetadataService;
import com.latticeengines.pls.service.DataFeedTaskManagerService;
import com.latticeengines.pls.workflow.CDLDataFeedImportWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataFeedTaskManagerService")
public class DataFeedTaskManagerServiceImpl implements DataFeedTaskManagerService {

    private static final Log log = LogFactory.getLog(DataFeedTaskManagerServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CDLDataFeedImportWorkflowSubmitter cdlDataFeedImportWorkflowSubmitter;

    @Override
    public String createDataFeedTask(String feedType, String entity, String source, String datafeedName,
                                           String metadata) {
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(source);
        Table newMeta = dataFeedMetadataService.getMetadata(metadata);
        CustomerSpace customerSpace = dataFeedMetadataService.getCustomerSpace(metadata);
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("Cannot find the tenant %s", customerSpace.getTenantId()));
        }
        MultiTenantContext.setTenant(tenant);
        DataFeedTask dataFeedTask = metadataProxy.getDataFeedTask(customerSpace.toString(), source, feedType, entity, datafeedName);
        if (dataFeedTask != null) {
            Table originMeta = dataFeedTask.getImportTemplate();
            if (!dataFeedMetadataService.compareMetadata(originMeta, newMeta)) {
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);

                //metadataProxy.createTable(customerSpace.toString(), newMeta.getName(), newMeta);
                dataFeedTask.setImportTemplate(newMeta);
                metadataProxy.updateDataFeedTask(customerSpace.toString(), dataFeedTask);
            }
            return getIdentifierFromDataFeedTask(datafeedName, dataFeedTask);
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
            metadataProxy.createDataFeedTask(customerSpace.toString(), datafeedName, dataFeedTask);
            return getIdentifierFromDataFeedTask(datafeedName, dataFeedTask);
        }
    }

    @Override
    public String submitImportJob(String taskIdentifier, String importConfig) {
        String[] keys = getTaskKeyFromIdentifier(taskIdentifier);
        DataFeedMetadataService dataFeedMetadataService = DataFeedMetadataService.getService(keys[0]);
        CustomerSpace customerSpace = dataFeedMetadataService.getCustomerSpace(importConfig);
        DataFeedTask dataFeedTask = metadataProxy.getDataFeedTask(customerSpace.toString(), keys[0], keys[2],
                keys[1], keys[3]);
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

    private DataFeed getDataFeed(String customerSpace, String feedName) {
        DataFeed dataFeed = metadataProxy.findDataFeedByName(customerSpace, feedName);
        if (dataFeed == null) {
            throw new RuntimeException(String.format("Cannot find data feed %s!", feedName));
        }

//        DataFeed datafeed = new DataFeed();
//        DataCollection dataCollection = new DataCollection();
//        dataCollection.setName("DATA_COLLECTION_NAME");
//        dataCollection.setTables(Collections.singletonList(table));
//        dataCollection.setType(DataCollectionType.Segmentation);
//
//        datafeed.setName(feedName);
//        datafeed.setStatus(DataFeed.Status.Active);
//        datafeed.setActiveExecution(1L);
//        datafeed.setDataCollection(dataCollection);
//        dataCollection.addDataFeed(datafeed);
        return dataFeed;
    }
}
