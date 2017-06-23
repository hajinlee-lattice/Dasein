package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.util.DataFeedImportUtils;
import com.latticeengines.metadata.dao.DataFeedDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedImportEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("datafeedEntityMgr")
public class DataFeedEntityMgrImpl extends BaseEntityMgrImpl<DataFeed> implements DataFeedEntityMgr {

    private static final Logger log = Logger.getLogger(DataFeedEntityMgrImpl.class);

    @Autowired
    private DataFeedDao datafeedDao;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Autowired
    private DataFeedImportEntityMgr datafeedImportEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public BaseDao<DataFeed> getDao() {
        return datafeedDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataFeed datafeed) {
        String dataCollectionName = datafeed.getDataCollection().getName();
        datafeed.setTenant(MultiTenantContext.getTenant());
        DataCollection dataCollection = dataCollectionEntityMgr.getDataCollection(dataCollectionName);
        if (dataCollection == null) {
            throw new IllegalStateException("Data collection " + dataCollectionName + " does not exist");
        }
        datafeed.setDataCollection(dataCollection);
        datafeed.setStatus(Status.Initing);
        super.create(datafeed);
        log.info(String.format("creating data feed tasks %s.", datafeed.getTasks()));
        for (DataFeedTask task : datafeed.getTasks()) {
            task.setDataFeed(datafeed);
            datafeedTaskEntityMgr.create(task);
        }
        DataFeedExecution execution = new DataFeedExecution();
        execution.setDataFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Active);
        datafeedExecutionEntityMgr.create(execution);
        datafeed.setActiveExecutionId(execution.getPid());
        datafeed.setActiveExecution(execution);
        log.info(String.format("created data feed %s.", datafeed));
        update(datafeed);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByName(String datafeedName) {
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
        datafeed.setActiveExecution(execution);
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
        List<DataFeedTask> tasks = new ArrayList<>(datafeed.getTasks());
        tasks.forEach(task -> task.setImportData(datafeedTaskEntityMgr.pollFirstDataTable(task)));
        List<DataFeedImport> imports = tasks.stream().map(DataFeedImportUtils::createImportFromTask)
                .collect(Collectors.toList());
        DataFeedExecution execution = datafeed.getActiveExecution();
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.addImports(imports);
        for (DataFeedImport datafeedImport : imports) {
            datafeedImportEntityMgr.create(datafeedImport);
        }
        log.info(String.format("starting execution %s", execution));
        datafeedExecutionEntityMgr.update(execution);

        datafeed.setActiveExecution(execution);
        if (datafeed.getStatus() == Status.Active) {
            datafeed.setStatus(Status.Consolidating);
        }
        tasks = datafeed.getTasks();
        tasks.forEach(task -> {
            datafeedTaskEntityMgr.update(task, null, new Date());
        });
        log.info(String.format("starting execution: updating data feed to %s", datafeed));
        datafeedDao.update(datafeed);
        return execution;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status) {
        DataFeed datafeed = findByNameInflated(datafeedName);
        if (datafeed == null) {
            log.error("Can't find data feed: " + datafeedName);
            return null;
        }
        DataFeedExecution execution = datafeed.getActiveExecution();
        execution.setStatus(status);
        datafeedExecutionEntityMgr.update(execution);

        DataFeedExecution newExecution = new DataFeedExecution();
        newExecution.setDataFeed(datafeed);
        newExecution.setStatus(DataFeedExecution.Status.Active);
        datafeedExecutionEntityMgr.create(newExecution);

        datafeed.setActiveExecutionId(newExecution.getPid());
        if (datafeed.getStatus() == Status.InitialLoaded) {
            if (status == DataFeedExecution.Status.Consolidated) {
                datafeed.setStatus(Status.InitialConsolidated);
            }
        } else if (datafeed.getStatus() == Status.Consolidating) {
            datafeed.setStatus(Status.Active);
        }
        log.info(String.format("terminating execution, updating data feed %s to %s", datafeedName, datafeed));
        datafeedDao.update(datafeed);
        return execution;
    }

}
