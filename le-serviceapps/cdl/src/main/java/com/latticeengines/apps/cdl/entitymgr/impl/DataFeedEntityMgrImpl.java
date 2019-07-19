package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataFeedDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataFeedRepository;
import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("datafeedEntityMgr")
public class DataFeedEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataFeed, Long> implements DataFeedEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataFeedEntityMgrImpl.class);

    @Inject
    private DataFeedRepository datafeedRepository;

    @Inject
    private DataFeedDao datafeedDao;

    @Inject
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

    @Inject
    private DataFeedTaskEntityMgr datafeedTaskEntityMgr;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Override
    public BaseDao<DataFeed> getDao() {
        return datafeedDao;
    }

    @Override
    public BaseJpaRepository<DataFeed, Long> getRepository() {
        return datafeedRepository;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(DataFeed datafeed) {
        DataCollection dataCollection;
        if (datafeed.getDataCollection() == null || StringUtils.isBlank(datafeed.getDataCollection().getName())) {
            dataCollection = dataCollectionEntityMgr.findDefaultCollection();
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
    public DataFeed findByPid(Long pid) {
        if (pid == null) {
            return null;
        }

        return datafeedRepository.findById(pid).orElse(null);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findByName(String datafeedName) {
        datafeedName = StringUtils.isBlank(datafeedName) ? findDefaultFeed().getName() : datafeedName;
        DataFeed datafeed = datafeedRepository.findByName(datafeedName);
        if (datafeed == null) {
            return null;
        }
        Long executionId = datafeed.getActiveExecutionId();
        if (executionId == null) {
            return datafeed;
        }
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByPid(executionId);
        if (execution != null) {
            datafeed.setActiveExecution(execution);
        }
        return datafeed;
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
        DataFeed datafeed = findByName(datafeedName);
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
        log.info(String.format("terminating execution, updating data feed %s to %s", datafeedName,
                JsonUtils.serialize(datafeed)));
        datafeedDao.update(datafeedDao.merge(datafeed));
        return execution;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public DataFeedExecution updateExecutionWithTerminalStatus(String datafeedName, DataFeedExecution.Status status,
                                                               Status datafeedStatus, Long executionId) {
        DataFeed datafeed = findByName(datafeedName);
        if (datafeed == null) {
            log.error("Can't find data feed: " + datafeedName);
            return null;
        }
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByPid(executionId);
        execution.setStatus(status);
        datafeedExecutionEntityMgr.update(execution);

        boolean flag = true;
        if (execution.getDataFeedExecutionJobType() == DataFeedExecutionJobType.CDLOperation) {
            if (execution.getPid() == datafeed.getActiveExecution().getPid()) {
                List<DataFeedExecution> activeExecutions =
                        datafeedExecutionEntityMgr.findActiveExecutionByDataFeedAndJobType(datafeed, DataFeedExecutionJobType.CDLOperation);
                if (!CollectionUtils.isEmpty(activeExecutions)) {
                    DataFeedExecution nextExecution = activeExecutions.get(0);
                    datafeed.setActiveExecution(nextExecution);
                    datafeed.setActiveExecutionId(nextExecution.getPid());
                    flag = false;
                } else {
                    datafeed.setStatus(datafeedStatus);
                }
            }
        } else {
            datafeed.setStatus(datafeedStatus);
        }

        if (DataFeedExecution.Status.Completed == status && Status.Active == datafeedStatus && flag) {
            datafeed.setLastPublished(new Date());
        }
        log.info(String.format("terminating execution, updating data feed %s to %s", datafeedName,
                JsonUtils.serialize(datafeed)));
        datafeedDao.update(datafeedDao.merge(datafeed));
        return execution;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findDefaultFeed() {
        DataCollection collection = dataCollectionEntityMgr.findDefaultCollection();
        if (collection == null) {
            throw new IllegalStateException("Default collection has not been initialized.");
        }
        return datafeedRepository.findByDataCollection(collection);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeed findDefaultFeedReadOnly() {
        DataCollection collection;
        try {
            collection = dataCollectionEntityMgr.findDefaultCollection();
        } catch (RuntimeException e) {
            collection = null;
        }
        if (collection == null) {
            return null;
        } else {
            DataFeed dataFeed = datafeedRepository.findByDataCollection(collection);
            return findByNameInflated(dataFeed.getName());
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataFeed> getAllDataFeeds() {
        List<DataFeed> dataFeeds = findAll();
        for (DataFeed datafeed : dataFeeds) {
            HibernateUtils.inflateDetails(datafeed.getTasks());
            for (DataFeedTask datafeedTask : datafeed.getTasks()) {
                TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
                TableEntityMgr.inflateTable(datafeedTask.getImportData());
            }
            if (datafeed.getActiveExecutionId() != null) {
                DataFeedExecution execution = datafeedExecutionEntityMgr.findByPid(datafeed.getActiveExecutionId());
                if (execution != null) {
                    datafeed.setActiveExecution(execution);
                }
            }
        }
        return dataFeeds;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SimpleDataFeed> getAllSimpleDataFeeds() {
        List<DataFeed> dataFeeds = findAll();
        return dataFeeds.stream().map(df -> new SimpleDataFeed(df.getTenant(), df.getStatus(), df.getNextInvokeTime(),
                df.isScheduleNow(), df.getScheduleTime(), df.getScheduleRequest()))
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SimpleDataFeed> getSimpleDataFeeds(TenantStatus status, String version) {
        List<DataFeed> dataFeeds = datafeedRepository.getDataFeedsByTenantStatus(status, version);
        return dataFeeds.stream().map(df -> new SimpleDataFeed(df.getTenant(), df.getStatus(), df.getNextInvokeTime(),
                df.isScheduleNow(), df.getScheduleTime(), df.getScheduleRequest()))
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataFeed> getDataFeeds(TenantStatus status, String version) {
        return datafeedRepository.getDataFeedsByTenantStatus(status, version);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataFeed> getDataFeedsBySchedulingGroup(TenantStatus status, String version, String schedulingGroup) {
        return datafeedRepository.getDataFeedsByTenantStatusAndSchedulingType(status, version, schedulingGroup);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public DataFeed updateStatus(DataFeed feed) {
        return datafeedDao.updateStatus(feed);
    }
}
