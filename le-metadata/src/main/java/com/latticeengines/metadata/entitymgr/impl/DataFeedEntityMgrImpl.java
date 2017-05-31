package com.latticeengines.metadata.entitymgr.impl;

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
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution.Status;
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
    private DataFeedTaskEntityMgr dataFeedTaskEntityMgr;

    @Override
    public BaseDao<DataFeed> getDao() {
        return datafeedDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataFeed datafeed) {
        datafeed.setTenant(MultiTenantContext.getTenant());
        super.create(datafeed);
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
    public boolean startExecution(String datafeedName) {
        DataFeed datafeed = datafeedDao.findByField("name", datafeedName);
        if (datafeed == null) {
            log.info("Can't find data feed: " + datafeedName);
            return false;
        }
        List<DataFeedTask> tasks = HibernateUtils.inflateDetails(datafeed.getTasks());
        datafeed.getTasks().forEach(task -> {
            task.setImportData(dataFeedTaskEntityMgr.pollFirstDataTable(task.getPid()));
        });
        List<DataFeedImport> imports = tasks.stream().map(DataFeedImportUtils::createImportFromTask)
                .collect(Collectors.toList());
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByExecutionId(datafeed.getActiveExecution());
        execution.setStatus(Status.Started);
        execution.addImports(imports);
        datafeedExecutionEntityMgr.update(execution);

        DataFeedExecution newExecution = new DataFeedExecution();
        newExecution.setFeed(datafeed);
        newExecution.setStatus(Status.Active);
        datafeedExecutionEntityMgr.create(newExecution);

        datafeed.addExeuction(newExecution);
        datafeed.setActiveExecution(newExecution.getPid());
        tasks.forEach(task -> {
            task.setStartTime(new Date());
            task.setImportData(null);
        });
        datafeedDao.update(datafeed);
        return true;
    }

}
