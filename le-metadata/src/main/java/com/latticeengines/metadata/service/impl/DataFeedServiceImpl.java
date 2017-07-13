package com.latticeengines.metadata.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedProfileEntityMgr;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.DataFeedService;

@Component("datafeedService")
public class DataFeedServiceImpl implements DataFeedService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedServiceImpl.class);

    @Autowired
    private DataFeedEntityMgr datafeedEntityMgr;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Autowired
    private DataFeedProfileEntityMgr datafeedProfileEntityMgr;

    @Autowired
    private DataCollectionService dataCollectionService;

    @Override
    public DataFeedExecution startExecution(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.startExecution(datafeedName);
    }

    @Override
    public DataFeed findDataFeedByName(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.findByNameInflated(datafeedName);
    }

    @Override
    public DataFeedExecution finishExecution(String customerSpace, String datafeedName, String initialDataFeedStatus) {

        return datafeedEntityMgr.updateExecutionWithTerminalStatus(datafeedName, DataFeedExecution.Status.Consolidated,
                getSuccessfulDataFeedStatus(initialDataFeedStatus));
    }

    @Override
    public DataFeed createDataFeed(String customerSpace, String collectionName, DataFeed datafeed) {
        if (StringUtils.isBlank(collectionName)) {
            collectionName = dataCollectionService.getOrCreateDefaultCollection(customerSpace).getName();
        }
        DataFeed existing = datafeedEntityMgr.findByName(datafeed.getName());
        if (existing != null) {
            log.warn("There is already a data feed called " + datafeed.getName() + ". Delete it first.");
            datafeedEntityMgr.delete(existing);
        }
        DataCollection shellCollection = new DataCollection();
        shellCollection.setName(collectionName);
        datafeed.setDataCollection(shellCollection);
        log.info("Creating a new datafeed named " + datafeed.getName() + " in collection " + collectionName);
        datafeedEntityMgr.create(datafeed);
        return datafeed;
    }

    @Override
    public DataFeed getOrCreateDataFeed(String customerSpace) {
        dataCollectionService.getOrCreateDefaultCollection(customerSpace);
        DataFeed dataFeed = datafeedEntityMgr.findDefaultFeed();
        return findDataFeedByName(customerSpace, dataFeed.getName());
    }

    @Override
    public void updateDataFeed(String customerSpace, String datafeedName, String statusStr) {
        DataFeed datafeed = findDataFeedByName(customerSpace, datafeedName);
        if (datafeed == null) {
            throw new NullPointerException("Datafeed is null. Cannot update status.");
        } else {
            Status status = Status.fromName(statusStr);
            datafeed.setStatus(status);
            datafeedEntityMgr.update(datafeed);
        }
    }

    @Override
    public DataFeedExecution failExecution(String customerSpace, String datafeedName, String initialDataFeedStatus) {
        return datafeedEntityMgr.updateExecutionWithTerminalStatus(datafeedName, DataFeedExecution.Status.Failed,
                getFailedDataFeedStatus(initialDataFeedStatus));
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

    public Status getSuccessfulDataFeedStatus(String initialDataFeedStatus) {
        Status datafeedStatus = Status.fromName(initialDataFeedStatus);
        if (datafeedStatus == Status.InitialLoaded || datafeedStatus == Status.InitialConsolidated) {
            return Status.InitialConsolidated;
        } else if (datafeedStatus == Status.Active) {
            return Status.Active;
        }
        throw new RuntimeException(
                String.format("Can't finish this execution due to datafeed status is %s", initialDataFeedStatus));
    }

    public Status getFailedDataFeedStatus(String initialDataFeedStatus) {
        Status datafeedStatus = Status.fromName(initialDataFeedStatus);
        if (datafeedStatus == Status.InitialLoaded) {
            return Status.InitialLoaded;
        }
        if (datafeedStatus == Status.InitialConsolidated) {
            return Status.InitialConsolidated;
        } else if (datafeedStatus == Status.Active) {
            return Status.Active;
        }
        throw new RuntimeException(
                String.format("Can't finish this execution due to datafeed status is %s", initialDataFeedStatus));
    }

    @Override
    public DataFeedExecution retryLatestExecution(String customerSpace, String datafeedName) {
        DataFeed datafeed = datafeedEntityMgr.findByNameInflated(datafeedName);
        if (datafeed == null) {
            return null;
        }
        return datafeedEntityMgr.retryLatestExecution(datafeed.getName());
    }

    @Override
    public DataFeedProfile startProfile(String customerSpace, String datafeedName) {
        return datafeedEntityMgr.startProfile(datafeedName);
    }

    @Override
    public DataFeedProfile updateProfileWorkflowId(String customerSpace, String datafeedName, Long workflowId) {
        DataFeed datafeed = datafeedEntityMgr.findByNameInflated(datafeedName);
        if (datafeed == null) {
            return null;
        }
        DataFeedProfile profile = datafeed.getActiveProfile();
        profile.setWorkflowId(workflowId);
        datafeedProfileEntityMgr.update(profile);
        return profile;
    }
}
