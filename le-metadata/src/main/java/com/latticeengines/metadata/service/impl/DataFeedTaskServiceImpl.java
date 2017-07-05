package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.service.DataFeedService;
import com.latticeengines.metadata.service.DataFeedTaskService;

@Component("dataFeedTaskService")
public class DataFeedTaskServiceImpl implements DataFeedTaskService {

    @Autowired
    private DataFeedTaskEntityMgr dataFeedTaskEntityMgr;

    @Autowired
    private DataFeedService dataFeedService;

    @Override
    public void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        dataFeedTask.setDataFeed(dataFeed);
        dataFeedTaskEntityMgr.create(dataFeedTask);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, entity, dataFeed.getPid());
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
    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask);
    }

    @Override
    public void registerExtract(String customerSpace, String taskUniqueId, String tableName, Extract extract) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskUniqueId);
        dataFeedTaskEntityMgr.registerExtract(dataFeedTask, tableName, extract);
    }

    @Override
    public void registerExtracts(String customerSpace, String taskUniqueId, String tableName, List<Extract> extracts) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskUniqueId);
        dataFeedTaskEntityMgr.registerExtracts(dataFeedTask, tableName, extracts);
    }

    @Override
    public List<Extract> getExtractsPendingInQueue(String customerSpace, String source, String dataFeedType,
            String entity) {
        DataFeedTask datafeedTask = getDataFeedTask(customerSpace, source, dataFeedType, entity);
        return dataFeedTaskEntityMgr.getExtractsPendingInQueue(datafeedTask);
    }
}
