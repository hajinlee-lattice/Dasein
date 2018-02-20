package com.latticeengines.metadata.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.DataFeedService;
import com.latticeengines.metadata.service.DataFeedTaskService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("datafeedService")
public class DataFeedServiceImpl implements DataFeedService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedServiceImpl.class);

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataFeedTaskService datafeedTaskService;

    @Inject
    private WorkflowProxy workflowProxy;

    private static final String LockType = "DataFeedExecutionLock";

    @Override
    public boolean lockExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobType) {
        String lockName = new Path("/").append(LockType).append("/").append(customerSpace).toString();
        try {
            LockManager.registerCrossDivisionLock(lockName);
            LockManager.acquireWriteLock(lockName, 5, TimeUnit.MINUTES);
            DataFeed datafeed = datafeedEntityMgr.findByNameInflated(datafeedName);
            Collection<DataFeedExecutionJobType> allowedJobType = datafeed.getStatus().getAllowedJobTypes();
            if (!allowedJobType.contains(jobType)) {
                DataFeedExecution execution = datafeed.getActiveExecution();
                if (execution == null || execution.getWorkflowId() == null
                        || !DataFeedExecution.Status.Started.equals(execution.getStatus())) {
                    return false;
                }
                Job job = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()), customerSpace);
                if (job == null || job.getJobStatus() == null || !job.getJobStatus().isTerminated()) {
                    return false;
                }
                failExecution(customerSpace, datafeedName,
                        job.getInputs().get(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS));
            }
            prepareExecution(customerSpace, datafeedName, jobType);
            return true;
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            return false;
        } finally {
            LockManager.releaseWriteLock(lockName);
        }
    }

    private void prepareExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobType) {
        datafeedEntityMgr.prepareExecution(customerSpace, datafeedName, jobType);
    }

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

        return datafeedEntityMgr.updateExecutionWithTerminalStatus(datafeedName, DataFeedExecution.Status.Completed,
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
    public synchronized DataFeed getOrCreateDataFeed(String customerSpace) {
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
    public List<SimpleDataFeed> getAllSimpleDataFeeds() {
        return datafeedEntityMgr.getAllSimpleDataFeeds();
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
