package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
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
        if (!CollectionUtils.isEmpty(datafeed.getExecutions())) {
            DataFeedExecution execution = datafeed.getExecutions().get(0);
            datafeedExecutionEntityMgr.create(execution);
            datafeed.setActiveExecution(execution.getPid());
            update(datafeed);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByName(String datafeedName) {
        DataFeed datafeed = findByField("name", datafeedName);
        if (datafeed != null) {
            HibernateUtils.inflateDetails(datafeed.getExecutions());
            HibernateUtils.inflateDetails(datafeed.getTasks());
        }
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
        List<DataFeedTask> tasks = new ArrayList<>(HibernateUtils.inflateDetails(datafeed.getTasks()));
        datafeed.getTasks().forEach(task -> {
            task.setImportData(datafeedTaskEntityMgr.pollFirstDataTable(task.getPid()));
        });
        List<DataFeedImport> imports = tasks.stream().map(DataFeedImportUtils::createImportFromTask)
                .collect(Collectors.toList());
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByExecutionId(datafeed.getActiveExecution());
        execution.setStatus(DataFeedExecution.Status.Started);
        execution.addImports(imports);
        datafeedExecutionEntityMgr.update(execution);

        DataFeedExecution newExecution = new DataFeedExecution();
        newExecution.setFeed(datafeed);
        newExecution.setStatus(DataFeedExecution.Status.Active);
        datafeedExecutionEntityMgr.create(newExecution);

        datafeed.addExeuction(newExecution);
        datafeed.setActiveExecution(newExecution.getPid());
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

        datafeed.setStatus(Status.Active);
        datafeedDao.update(datafeed);
        return execution;
    }

}
