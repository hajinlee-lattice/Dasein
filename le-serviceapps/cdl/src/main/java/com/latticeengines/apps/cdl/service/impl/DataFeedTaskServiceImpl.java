package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.metadata.service.MetadataService;

@Component("dataFeedTaskService")
public class DataFeedTaskServiceImpl implements DataFeedTaskService {

    @Inject
    private DataFeedTaskEntityMgr dataFeedTaskEntityMgr;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private MetadataService mdService;

    @Override
    public void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        dataFeedTask.setDataFeed(dataFeed);
        dataFeedTaskEntityMgr.create(dataFeedTask);
    }

    @Override
    public void createOrUpdateDataFeedTask(String customerSpace, String source, String dataFeedType, String entity,
            String tableName) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        DataFeedTask dataFeedTask = dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, entity, dataFeed);
        if (dataFeedTask == null) {
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setDataFeed(dataFeed);
            Table metaData = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, true);
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedTask.setImportTemplate(metaData);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity);
            dataFeedTask.setFeedType(dataFeedType);
            dataFeedTask.setSource(source);
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTaskEntityMgr.create(dataFeedTask);
        } else {
            if (!dataFeedTask.getImportTemplate().getName().equals(tableName)) {
                Table metaData = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, true);
                dataFeedTask.setImportTemplate(metaData);
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);
                dataFeedTask.setLastUpdated(new Date());
                dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask);
            }
        }
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, entity, dataFeed);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, Long taskId) {
        return dataFeedTaskEntityMgr.getDataFeedTask(taskId);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String uniqueId) {
        return dataFeedTaskEntityMgr.getDataFeedTask(uniqueId);
    }

    @Override
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String customerSpace, String entity) {
        DataFeed datafeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (datafeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTaskWithSameEntity(entity, datafeed);
    }

    @Override
    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask);
    }

    @Override
    public List<String> registerExtract(String customerSpace, String taskUniqueId, String tableName, Extract extract) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskUniqueId);
        DataFeed dataFeed = dataFeedTask.getDataFeed();
        if (dataFeed.getStatus() == DataFeed.Status.Initing) {
            // log.info("Skip registering extract for feed in initing state");
            return Collections.emptyList();
        }
        return dataFeedTaskEntityMgr.registerExtract(dataFeedTask, tableName, extract);
    }

    @Override
    public List<String> registerExtracts(String customerSpace, String taskUniqueId, String tableName, List<Extract> extracts) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskUniqueId);
        DataFeed dataFeed = dataFeedTask.getDataFeed();
        if (dataFeed.getStatus() == DataFeed.Status.Initing) {
            // log.info("Skip registering extract for feed in initing state");
            return Collections.emptyList();
        }
        return dataFeedTaskEntityMgr.registerExtracts(dataFeedTask, tableName, extracts);
    }

    @Override
    public void addTableToQueue(String customerSpace, String taskUniqueId, String tableName) {
        dataFeedTaskEntityMgr.addTableToQueue(taskUniqueId, tableName);
    }

    @Override
    public void addTablesToQueue(String customerSpace, String taskUniqueId, List<String> tableNames) {
        dataFeedTaskEntityMgr.addTablesToQueue(taskUniqueId, tableNames);
    }

    @Override
    public List<Extract> getExtractsPendingInQueue(String customerSpace, String source, String dataFeedType,
            String entity) {
        DataFeedTask datafeedTask = getDataFeedTask(customerSpace, source, dataFeedType, entity);
        return dataFeedTaskEntityMgr.getExtractsPendingInQueue(datafeedTask);
    }

    @Override
    public void resetImport(String customerSpaceStr, DataFeedTask datafeedTask) {
        dataFeedTaskEntityMgr.clearTableQueuePerTask(datafeedTask);
    }
}
