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
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.util.DataFeedImportUtils;
import com.latticeengines.metadata.dao.DataFeedDao;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("datafeedEntityMgr")
public class DataFeedEntityMgrImpl extends BaseEntityMgrImpl<DataFeed> implements DataFeedEntityMgr {

    private static final Logger log = Logger.getLogger(DataFeedEntityMgrImpl.class);
    @Autowired
    private DataFeedDao datafeedDao;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Override
    public BaseDao<DataFeed> getDao() {
        return datafeedDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataFeed datafeed) {
        datafeed.setTenant(MultiTenantContext.getTenant());
        super.create(datafeed);
        for (DataFeedTask task : datafeed.getTasks()) {
            datafeedTaskEntityMgr.create(task);
        }
        DataFeedExecution execution = new DataFeedExecution();
        execution.setFeed(datafeed);
        execution.setStatus(DataFeedExecution.Status.Active);
        datafeedExecutionEntityMgr.create(execution);
        datafeed.setActiveExecutionId(execution.getPid());
        update(datafeed);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByName(String datafeedName) {
        DataFeed datafeed = findByField("name", datafeedName);
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByExecutionId(datafeed.getActiveExecutionId());
        datafeed.setActiveExecution(execution);
        if (datafeed != null) {
            HibernateUtils.inflateDetails(datafeed.getTasks());

        }
        return datafeed;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByNameWithAllExecutions(String datafeedName) {
        DataFeed datafeed = findByName(datafeedName);
        datafeed.setExecutions(datafeedExecutionEntityMgr.findByDataFeed(datafeed));
        return datafeed;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DataFeedExecution startExecution(String datafeedName) {
        DataFeed datafeed = datafeedDao.findByField("name", datafeedName);
        if (datafeed == null) {
            log.info("Can't find data feed: " + datafeedName);
            return null;
        }
        List<DataFeedTask> tasks = new ArrayList<>(datafeed.getTasks());
        tasks.forEach(task -> {
            task.setImportData(datafeedTaskEntityMgr.pollFirstDataTable(task));
        });
        List<DataFeedImport> imports = tasks.stream().map(DataFeedImportUtils::createImportFromTask)
                .collect(Collectors.toList());
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByExecutionId(datafeed.getActiveExecutionId());
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.addImports(imports);
        datafeedExecutionEntityMgr.update(execution);

        datafeed.setActiveExecution(execution);
        datafeed.setStatus(Status.Consolidating);
        tasks.forEach(task -> {
            task.setStartTime(new Date());
            task.setImportData(null);
        });
        datafeedDao.update(datafeed);
        return execution;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status) {
        DataFeed datafeed = datafeedDao.findByField("name", datafeedName);
        if (datafeed == null) {
            log.error("Can't find data feed: " + datafeedName);
            return null;
        }
        DataFeedExecution execution = datafeedExecutionEntityMgr.findConsolidatingExecution(datafeed);
        execution.setStatus(status);
        datafeedExecutionEntityMgr.update(execution);

        DataFeedExecution newExecution = new DataFeedExecution();
        newExecution.setFeed(datafeed);
        newExecution.setStatus(DataFeedExecution.Status.Active);
        datafeedExecutionEntityMgr.create(newExecution);

        datafeed.setActiveExecutionId(newExecution.getPid());

        datafeed.setStatus(Status.Active);
        datafeedDao.update(datafeed);
        return execution;
    }

}
