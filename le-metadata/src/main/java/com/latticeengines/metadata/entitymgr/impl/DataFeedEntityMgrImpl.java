package com.latticeengines.metadata.entitymgr.impl;

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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.metadata.dao.DataFeedDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("datafeedEntityMgr")
public class DataFeedEntityMgrImpl extends BaseEntityMgrImpl<DataFeed> implements DataFeedEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataFeedEntityMgrImpl.class);

    @Autowired
    private DataFeedDao datafeedDao;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Autowired
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public BaseDao<DataFeed> getDao() {
        return datafeedDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
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
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByName(String datafeedName) {
        datafeedName = StringUtils.isBlank(datafeedName) ? findDefaultFeed().getName() : datafeedName;
        return findByField("name", datafeedName);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
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
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByPid(datafeed.getActiveExecutionId());
        if (execution != null) {
            datafeed.setActiveExecution(execution);
        }
        return datafeed;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByNameInflatedWithAllExecutions(String datafeedName) {
        DataFeed datafeed = findByNameInflated(datafeedName);
        datafeed.setExecutions(datafeedExecutionEntityMgr.findByDataFeed(datafeed));
        return datafeed;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
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
        if (DataFeedExecution.Status.Completed == status && Status.Active == datafeedStatus) {
            datafeed.setLastPublished(new Date());
        }
        log.info(String.format("terminating execution, updating data feed %s to %s", datafeedName, datafeed));
        datafeedDao.update(datafeed);
        return execution;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findDefaultFeed() {
        DataCollection collection = dataCollectionEntityMgr.getDefaultCollectionReadOnly();
        if (collection == null) {
            throw new IllegalStateException("Default collection has not been initialized.");
        }
        return datafeedDao.findDefaultFeed(collection.getName());
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
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
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataFeed> getAllDataFeeds() {
        List<DataFeed> dataFeeds = datafeedDao.findAll();
        for (DataFeed datafeed : dataFeeds) {
            HibernateUtils.inflateDetails(datafeed.getTasks());
            for (DataFeedTask datafeedTask : datafeed.getTasks()) {
                TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
                TableEntityMgr.inflateTable(datafeedTask.getImportData());
            }
            DataFeedExecution execution = datafeedExecutionEntityMgr.findByPid(datafeed.getActiveExecutionId());
            if (execution != null) {
                datafeed.setActiveExecution(execution);
            }
        }
        return dataFeeds;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SimpleDataFeed> getAllSimpleDataFeeds() {
        return datafeedDao.findAllSimpleDataFeeds();
    }
}
