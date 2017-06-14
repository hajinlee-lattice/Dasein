package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.service.DataFeedService;

@Component("datafeedService")
public class DataFeedServiceImpl implements DataFeedService {

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Override
    public DataFeedExecution startExecution(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.startExecution(datafeedName);
    }

    @Override
    public DataFeed findDataFeedByName(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.findByNameInflated(datafeedName);
    }

    @Override
    public DataFeedExecution finishExecution(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.updateExecutionWithTerminalStatus(datafeedName, DataFeedExecution.Status.Consolidated);
    }

    @Override
    public DataFeed createDataFeed(String customerSpace, DataFeed datafeed) {
        datafeedEntityMgr.create(datafeed);
        return datafeed;
    }

    @Override
    public void updateDataFeed(String customerSpace, String datafeedName, Status status) {
        DataFeed datafeed = findDataFeedByName(customerSpace, datafeedName);
        if (datafeed == null) {
            throw new NullPointerException("Datafeed is null. Cannot update status.");
        } else {
            datafeed.setStatus(status);
            datafeedEntityMgr.update(datafeed);
        }
    }

    @Override
    public DataFeedExecution failExecution(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.updateExecutionWithTerminalStatus(datafeedName, DataFeedExecution.Status.Failed);
    }

    @Override
    public DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId) {
        DataFeed datafeed = datafeedEntityMgr.findByNameInflated(datafeedName);
        if (datafeed == null) {
            return null;
        }
        DataFeedExecution execution = datafeed.getActiveExecution();
        execution.setWorkflowId(workflowId);
        datafeedExecutionEntityMgr.update(execution);
        return execution;
    }
}
