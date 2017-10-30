package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;
import com.latticeengines.domain.exposed.util.DataFeedImportUtils;
import com.latticeengines.metadata.dao.DataFeedDao;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedProfileEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("datafeedEntityMgr")
public class DataFeedEntityMgrImpl extends BaseEntityMgrImpl<DataFeed> implements DataFeedEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataFeedEntityMgrImpl.class);

    @Autowired
    private DataFeedDao datafeedDao;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Autowired
    private DataFeedProfileEntityMgr datafeedProfileEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private DataFeedTaskTableDao datafeedTaskTableDao;

    @Override
    public BaseDao<DataFeed> getDao() {
        return datafeedDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataFeed datafeed) {
        DataCollection dataCollection;
        if (datafeed.getDataCollection() == null || StringUtils.isBlank(datafeed.getDataCollection().getName())) {
            dataCollection = dataCollectionEntityMgr.getOrCreateDefaultCollection();
        } else {
            dataCollection = dataCollectionEntityMgr.getDataCollection(datafeed.getDataCollection().getName());
        }
        if (dataCollection == null) {
            throw new IllegalStateException("Cannot find the data collection that supposed to own the data feed.");
        }
        datafeed.setTenant(MultiTenantContext.getTenant());
        datafeed.setDataCollection(dataCollection);
        datafeed.setStatus(Status.Initing);
        super.create(datafeed);
        log.info(String.format("creating data feed tasks %s.", datafeed.getTasks()));
        for (DataFeedTask task : datafeed.getTasks()) {
            task.setDataFeed(datafeed);
            datafeedTaskEntityMgr.create(task);
        }
        log.info(String.format("created data feed %s.", datafeed));
        update(datafeed);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByName(String datafeedName) {
        datafeedName = StringUtils.isBlank(datafeedName) ? findDefaultFeed().getName() : datafeedName;
        return findByField("name", datafeedName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByNameInflated(String datafeedName) {
        DataFeed datafeed = findByName(datafeedName);
        if (datafeed == null) {
            return null;
        }
        HibernateUtils.inflateDetails(datafeed.getTasks());
        for (DataFeedTask datafeedTask : datafeed.getTasks()) {
            TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(datafeedTask.getImportData());
        }
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByExecutionId(datafeed.getActiveExecutionId());
        if (execution != null) {
            datafeed.setActiveExecution(execution);
        }
        DataFeedProfile profile = datafeedProfileEntityMgr.findByProfileId(datafeed.getActiveProfileId());
        if (profile != null) {
            datafeed.setActiveProfile(profile);
        }
        return datafeed;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByNameInflatedWithAllExecutions(String datafeedName) {
        DataFeed datafeed = findByNameInflated(datafeedName);
        datafeed.setExecutions(datafeedExecutionEntityMgr.findByDataFeed(datafeed));
        return datafeed;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DataFeedExecution startExecution(String datafeedName) {
        DataFeed datafeed = findByNameInflated(datafeedName);
        if (datafeed == null) {
            log.info("Can't find data feed: " + datafeedName);
            return null;
        }

        List<DataFeedImport> imports = new ArrayList<>();
        List<DataFeedTask> tasks = new ArrayList<>(datafeed.getTasks());
        tasks.forEach(task -> {
            imports.addAll(createImports(task));
            datafeedTaskEntityMgr.clearTableQueuePerTask(task);
        });
        log.info("imports for consolidates are: " + imports);

        DataFeedExecution execution = new DataFeedExecution();
        execution.setDataFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.addImports(imports);
        log.info(String.format("starting execution %s", execution));
        datafeedExecutionEntityMgr.create(execution);

        datafeed.setActiveExecutionId(execution.getPid());
        datafeed.setActiveExecution(execution);
        datafeed.setStatus(Status.Consolidating);
        tasks = datafeed.getTasks();
        tasks.forEach(task -> {
            datafeedTaskEntityMgr.update(task, new Date());
        });
        log.info(String.format("starting execution: updating data feed to %s", datafeed));

        update(datafeed);
        return execution;
    }

    private List<DataFeedImport> createImports(DataFeedTask task) {
        List<DataFeedImport> imports = new ArrayList<>();

        List<DataFeedTaskTable> datafeedTaskTables = datafeedTaskTableDao.getDataFeedTaskTables(task);
        datafeedTaskTables.stream().map(DataFeedTaskTable::getTable).forEach(dataTable -> {
            TableEntityMgr.inflateTable(dataTable);
            if (dataTable != null) {
                if (!dataTable.getExtracts().isEmpty()) {
                    task.setImportData(dataTable);
                    imports.add(DataFeedImportUtils.createImportFromTask(task));
                } else {
                    log.info(String.format("skip table: %s as this table extract is empty", dataTable.getName()));
                }
            }
        });
        return imports;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DataFeedExecution retryLatestExecution(String datafeedName) {
        DataFeed datafeed = findByNameInflated(datafeedName);
        DataFeedExecution execution = datafeed.getActiveExecution();
        execution.setStatus(DataFeedExecution.Status.Started);
        log.info(String.format("restarting execution %s", execution));
        datafeedExecutionEntityMgr.update(execution);

        datafeed.setStatus(Status.Consolidating);
        log.info(String.format("restarting execution: updating data feed to %s", datafeed));
        update(datafeed);
        return execution;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status,
            Status datafeedStatus) {
        DataFeed datafeed = findByNameInflated(datafeedName);
        if (datafeed == null) {
            log.error("Can't find data feed: " + datafeedName);
            return null;
        }
        DataFeedExecution execution = datafeed.getActiveExecution();
        execution.setStatus(status);
        datafeedExecutionEntityMgr.update(execution);

        datafeed.setStatus(datafeedStatus);
        if (DataFeedExecution.Status.Consolidated == status && Status.Active == datafeedStatus) {
            datafeed.setLastPublished(new Date());
        }
        log.info(String.format("terminating execution, updating data feed %s to %s", datafeedName, datafeed));
        datafeedDao.update(datafeed);
        return execution;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findDefaultFeed() {
        DataCollection collection = dataCollectionEntityMgr.getDefaultCollectionReadOnly();
        if (collection == null) {
            throw new IllegalStateException("Default collection has not been initialized.");
        }
        return datafeedDao.findDefaultFeed(collection.getName());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findDefaultFeedReadOnly() {
        DataCollection collection = null;
        try {
            collection = dataCollectionEntityMgr.getDefaultCollectionReadOnly();
        } catch (RuntimeException e) {
            collection = null;
        }
        if (collection == null) {
            return null;
        } else {
            DataFeed dataFeed = datafeedDao.findDefaultFeed(collection.getName());
            return findByNameInflated(dataFeed.getName());
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DataFeedProfile startProfile(String datafeedName) {
        DataFeed datafeed = findByNameInflated(datafeedName);
        if (datafeed == null) {
            return null;
        }
        Long executionId = datafeed.getActiveExecutionId();
        DataFeedProfile profile = new DataFeedProfile();
        profile.setDataFeed(datafeed);
        profile.setLatestDataFeedExecutionId(executionId);
        datafeedProfileEntityMgr.create(profile);
        datafeed.setActiveProfileId(profile.getPid());
        datafeed.setActiveProfile(profile);
        datafeed.setStatus(Status.Profiling);
        update(datafeed);
        return profile;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataFeed> getAllDataFeeds() {
        List<DataFeed> dataFeeds = datafeedDao.findAll();
        for (DataFeed datafeed : dataFeeds) {
            HibernateUtils.inflateDetails(datafeed.getTasks());
            for (DataFeedTask datafeedTask : datafeed.getTasks()) {
                TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
                TableEntityMgr.inflateTable(datafeedTask.getImportData());
            }
            DataFeedExecution execution = datafeedExecutionEntityMgr.findByExecutionId(datafeed.getActiveExecutionId());
            if (execution != null) {
                datafeed.setActiveExecution(execution);
            }
            DataFeedProfile profile = datafeedProfileEntityMgr.findByProfileId(datafeed.getActiveProfileId());
            if (profile != null) {
                datafeed.setActiveProfile(profile);
            }
        }
        return dataFeeds;
    }

}
