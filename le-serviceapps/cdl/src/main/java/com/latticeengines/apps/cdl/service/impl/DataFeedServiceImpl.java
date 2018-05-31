package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;
import com.latticeengines.domain.exposed.metadata.datafeed.DrainingStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.util.DataFeedImportUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("datafeedService")
public class DataFeedServiceImpl implements DataFeedService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedServiceImpl.class);

    @Inject
    private DataFeedEntityMgr datafeedEntityMgr;

    @Inject
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Inject
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataFeedTaskService datafeedTaskService;

    @Inject
    private WorkflowProxy workflowProxy;

    private static final String LockType = "DataFeedExecutionLock";

    public DataFeedServiceImpl() {
    }

    @VisibleForTesting
    DataFeedServiceImpl(DataFeedEntityMgr datafeedEntityMgr, DataFeedExecutionEntityMgr datafeedExecutionEntityMgr,
            DataFeedTaskEntityMgr datafeedTaskEntityMgr, DataCollectionService dataCollectionService,
            DataFeedTaskService datafeedTaskService, WorkflowProxy workflowProxy) {
        this.datafeedEntityMgr = datafeedEntityMgr;
        this.datafeedExecutionEntityMgr = datafeedExecutionEntityMgr;
        this.datafeedTaskEntityMgr = datafeedTaskEntityMgr;
        this.dataCollectionService = dataCollectionService;
        this.datafeedTaskService = datafeedTaskService;
        this.workflowProxy = workflowProxy;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public boolean lockExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobType) {
        String lockName = new Path("/").append(LockType).append("/").append(customerSpace).toString();
        try {
            LockManager.registerCrossDivisionLock(lockName);
            LockManager.acquireWriteLock(lockName, 5, TimeUnit.MINUTES);
            DataFeed datafeed = datafeedEntityMgr.findByName(datafeedName);
            if (datafeed.getStatus().getDisallowedJobTypes().contains(jobType)) {
                log.info(String.format("job type %s is disallowed by current data feed status %s", jobType,
                        datafeed.getStatus()));
                return false;
            }
            if (!datafeed.getStatus().getAllowedJobTypes().contains(jobType)) {
                DataFeedExecution execution = datafeed.getActiveExecution();
                if (execution == null || execution.getWorkflowId() == null) {
                    log.info("can't lock execution due to either execution or workflowid is null");
                    return false;
                }
                Job job = workflowProxy.getWorkflowExecution(String.valueOf(execution.getWorkflowId()), customerSpace);
                if (job == null || job.getJobStatus() == null || !job.getJobStatus().isTerminated()) {
                    log.info("can't lock execution due to workflow job is not terminated yet");
                    return false;
                }
                failExecution(datafeed, job.getInputs().get(WorkflowContextConstants.Inputs.INITIAL_DATAFEED_STATUS));
            }
            prepareExecution(datafeed, jobType);
            return true;
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            return false;
        } finally {
            LockManager.releaseWriteLock(lockName);
        }
    }

    private DataFeedExecution prepareExecution(DataFeed datafeed, DataFeedExecutionJobType jobType) {
        DataFeedExecution execution = new DataFeedExecution();
        execution.setDataFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.setDataFeedExecutionJobType(jobType);
        datafeedExecutionEntityMgr.create(execution);
        log.info(String.format("preparing execution %s", execution));

        datafeed.setActiveExecutionId(execution.getPid());
        datafeed.setActiveExecution(execution);
        datafeed.setStatus(jobType.getRunningStatus());
        log.info(String.format("preparing execution: updating data feed %d", datafeed.getPid()));
        datafeedEntityMgr.update(datafeed);
        return execution;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public DataFeedExecution startExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobType,
            long jobId) {
        if (DataFeedExecutionJobType.PA == jobType) {
            return startPnAExecution(customerSpace, datafeedName, jobId);
        } else if (DataFeedExecutionJobType.CDLOperation == jobType) {
            DataFeedExecution execution = datafeedEntityMgr.findByName(datafeedName).getActiveExecution();
            execution.setWorkflowId(jobId);
            datafeedExecutionEntityMgr.update(execution);
            return execution;
        }
        return datafeedEntityMgr.findByName(datafeedName).getActiveExecution();
    }

    private DataFeedExecution startPnAExecution(String customerSpace, String datafeedName, long jobId) {
        DataFeed datafeed = datafeedEntityMgr.findByNameInflated(datafeedName);
        if (datafeed == null) {
            log.info("Can't find data feed");
            return null;
        }

        List<DataFeedImport> imports = new ArrayList<>();
        List<DataFeedTask> tasks = datafeed.getTasks();
        tasks.forEach(task -> {
            imports.addAll(createImports(task));
            datafeedTaskEntityMgr.clearTableQueuePerTask(task);
        });
        log.info("imports for processanalyze are: " + imports);

        Collections.sort(imports, (a, b) -> Long.compare(a.getDataTable().getPid(), b.getDataTable().getPid()));
        DataFeedExecution execution = datafeed.getActiveExecution();
        execution.addImports(imports);
        execution.setWorkflowId(jobId);
        log.info(String.format("starting processanalyze execution %s", execution));
        datafeedExecutionEntityMgr.updateImports(execution);

        tasks = datafeed.getTasks();
        tasks.forEach(task -> {
            datafeedTaskEntityMgr.update(task, new Date());
        });
        log.info(String.format("starting execution: updating data feed to %s", datafeed));

        datafeedEntityMgr.update(datafeed);
        return execution;
    }

    private List<DataFeedImport> createImports(DataFeedTask task) {
        List<DataFeedImport> imports = new ArrayList<>();

        List<DataFeedTaskTable> datafeedTaskTables = datafeedTaskEntityMgr.getInflatedDataFeedTaskTables(task);
        datafeedTaskTables.stream().map(DataFeedTaskTable::getTable)//
                .filter(Objects::nonNull)//
                .forEach(dataTable -> {
                    task.setImportData(dataTable);
                    imports.add(DataFeedImportUtils.createImportFromTask(task));
                });
        return imports;

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

    private DataFeedExecution failExecution(DataFeed datafeed, String initialDataFeedStatus) {
        DataFeedExecution execution = datafeed.getActiveExecution();
        execution.setStatus(DataFeedExecution.Status.Failed);
        datafeedExecutionEntityMgr.update(execution);

        datafeed.setStatus(getFailedDataFeedStatus(initialDataFeedStatus));
        log.info(String.format("updating data feed %s to %d", datafeed.getName(), datafeed.getPid()));
        datafeedEntityMgr.update(datafeed);
        return execution;
    }

    @Override
    public DataFeedExecution failExecution(String customerSpace, String datafeedName, String initialDataFeedStatus) {
        return datafeedEntityMgr.updateExecutionWithTerminalStatus(datafeedName, DataFeedExecution.Status.Failed,
                getFailedDataFeedStatus(initialDataFeedStatus));
    }

    @Override
    public DataFeedExecution updateExecutionWorkflowId(String customerSpace, String datafeedName, Long workflowId) {
        DataFeed datafeed = datafeedEntityMgr.findByName(datafeedName);
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
    public DataFeed updateEarliestLatestTransaction(String customerSpace, String datafeedName,
            Integer earliestDayPeriod, Integer latestDayPeriod) {
        DataFeed datafeed = findDataFeedByName(customerSpace, datafeedName);
        if (datafeed == null) {
            throw new NullPointerException("Datafeed is null. Cannot update status.");
        } else {
            datafeed.setEarliestTransaction(earliestDayPeriod);
            datafeed.setLatestTransaction(latestDayPeriod);
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

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public Long restartExecution(String customerSpace, String datafeedName, DataFeedExecutionJobType jobType) {
        DataFeed datafeed = datafeedEntityMgr.findByName(datafeedName);
        if (datafeedExecutionEntityMgr.countByDataFeedAndJobType(datafeed, jobType) == 0) {
            return null;
        }
        DataFeedExecution execution = datafeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(datafeed,
                jobType);
        if (!lockExecution(customerSpace, datafeedName, jobType)) {
            throw new RuntimeException("can't lock execution for job type:" + jobType);
        }

        return execution.getWorkflowId();
    }
}
