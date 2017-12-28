package com.latticeengines.metadata.service.impl;

import java.util.Date;
import java.util.List;

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
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedProfileEntityMgr;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.DataFeedService;
import com.latticeengines.metadata.service.DataFeedTaskService;

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

    @Autowired
    private DataFeedTaskService datafeedTaskService;

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

        return datafeedEntityMgr.updateExecutionWithTerminalStatus(datafeedName, DataFeedExecution.Status.ProcessAnalyzed,
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
    public DataFeed getDefaultDataFeed(String customerSpace) {
        return datafeedEntityMgr.findDefaultFeedReadOnly();
    }

    @Override
    public void updateDataFeedDrainingStatus(String customerSpace, String drainingStatusStr) {
        DataFeed dataFeed = getDefaultDataFeed(customerSpace);
        if (dataFeed != null) {
            dataFeed = findDataFeedByName(customerSpace, dataFeed.getName());
            DrainingStatus drainingStatus = DrainingStatus.valueOf(drainingStatusStr);
            dataFeed.setDrainingStatus(drainingStatus);
            datafeedEntityMgr.update(dataFeed);
        }
    }

    @Override
    public void updateDataFeedMaintenanceMode(String customerSpace, boolean maintenanceMode) {
        DataFeed datafeed = findDataFeedByName(customerSpace, "");
        if (datafeed == null) {
            throw new NullPointerException("Datafeed is null. Cannot update maintenance mode.");
        } else {
            datafeed.setMaintenanceMode(maintenanceMode);
            datafeedEntityMgr.update(datafeed);
        }
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

    @Override
    public void resetImport(String customerSpace, String datafeedName) {
        DataFeed dataFeed = datafeedEntityMgr.findByNameInflated(datafeedName);
        if (dataFeed == null) {
            return;
        }

        for (DataFeedTask task : dataFeed.getTasks()) {
            datafeedTaskService.resetImport(customerSpace, task);
        }
    }

    public Status getSuccessfulDataFeedStatus(String initialDataFeedStatus) {
        Status datafeedStatus = Status.fromName(initialDataFeedStatus);
        switch (datafeedStatus) {
        case InitialLoaded:
        case Active:
            return Status.Active;
        default:
            throw new RuntimeException(
                    String.format("Can't finish this execution due to datafeed status is %s", initialDataFeedStatus));
        }
    }

    public Status getFailedDataFeedStatus(String initialDataFeedStatus) {
        Status datafeedStatus = Status.fromName(initialDataFeedStatus);
        switch (datafeedStatus) {
        case InitialLoaded:
            return Status.InitialLoaded;
        case Active:
        case ProcessAnalyzing:
            return Status.Active;
        default:
            throw new RuntimeException(
                    String.format("Can't finish this execution due to datafeed status is %s", initialDataFeedStatus));
        }

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
    public DataFeed finishProfile(String customerSpace, String datafeedName, String statusStr) {
        DataFeed datafeed = findDataFeedByName(customerSpace, datafeedName);
        if (datafeed == null) {
            throw new NullPointerException("Datafeed is null. Cannot update status.");
        } else {
            Status status = Status.fromName(statusStr);
            datafeed.setStatus(status);
            datafeed.setLastProfiled(new Date());
            datafeed.setLastPublished(new Date());
            datafeedEntityMgr.update(datafeed);
        }
        return datafeed;
    }

    @Override
    public DataFeed updateEarliestTransaction(String customerSpace, String datafeedName, Integer transactionDayPeriod) {
        DataFeed datafeed = findDataFeedByName(customerSpace, datafeedName);
        if (datafeed == null) {
            throw new NullPointerException("Datafeed is null. Cannot update status.");
        } else {
            datafeed.setEarliestTransaction(transactionDayPeriod);
            datafeedEntityMgr.update(datafeed);
        }
        return datafeed;
    }

    @Override
    public DataFeed rebuildTransaction(String customerSpace, String datafeedName, Boolean isRebuild) {
        DataFeed datafeed = findDataFeedByName(customerSpace, datafeedName);
        if (datafeed == null) {
            throw new NullPointerException("Datafeed is null. Cannot update status.");
        } else {
            datafeed.setRebuildTransaction(isRebuild);
            datafeedEntityMgr.update(datafeed);
        }
        return datafeed;
    }

    @Override
    public List<DataFeed> getAllDataFeeds() {
        return datafeedEntityMgr.getAllDataFeeds();
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

    @Override
    public void resetImportByEntity(String customerSpace, String datafeedName, String entity) {
        DataFeed dataFeed = datafeedEntityMgr.findByNameInflated(datafeedName);
        if (dataFeed == null) {
            return;
        }

        for (DataFeedTask task : dataFeed.getTasks()) {
            if (task.getEntity().equals(entity)) {
                datafeedTaskService.resetImport(customerSpace, task);
            }
        }
    }
}
