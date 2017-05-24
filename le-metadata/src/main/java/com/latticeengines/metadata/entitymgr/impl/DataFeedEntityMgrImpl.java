package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.DataFeedImportUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.DataFeedDao;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("datafeedEntityMgr")
public class DataFeedEntityMgrImpl extends BaseEntityMgrImpl<DataFeed> implements DataFeedEntityMgr {

    @Autowired
    private DataFeedDao datafeedDao;

    @Autowired
    private DataFeedExecutionEntityMgr datafeedExecutionEntityMgr;

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
    @Transactional(propagation = Propagation.REQUIRED)
    public void startExecution(String datafeedName) {
        DataFeed datafeed = datafeedDao.findByField("name", datafeedName);
        if (datafeed == null) {
            return;
        }
        List<DataFeedTask> tasks = HibernateUtils.inflateDetails(datafeed.getTasks());

        List<DataFeedImport> imports = tasks.stream().map(DataFeedImportUtils::createImportFromTask)
                .collect(Collectors.toList());
        DataFeedExecution execution = datafeedExecutionEntityMgr.findByField("execution",
                datafeed.getActiveExecution());
        execution.setStatus(Status.Inited);
        execution.addImports(imports);
        datafeedExecutionEntityMgr.update(execution);

        DataFeedExecution newExecution = new DataFeedExecution();
        newExecution.setExecution(datafeed.getActiveExecution() + 1);
        newExecution.setFeed(datafeed);
        newExecution.setStatus(Status.Active);
        datafeedExecutionEntityMgr.create(newExecution);

        datafeed.addExeuction(newExecution);
        datafeed.setActiveExecution(newExecution.getExecution());
        tasks.forEach(task -> {
            task.setStartTime(new Date());
            Table dataTable = task.getImportData();
            Table newDatatTable = TableUtils.clone(dataTable,
                    "datatable_" + UUID.randomUUID().toString().replace('-', '_'));
            newDatatTable.setExtracts(new ArrayList<>());
            newDatatTable.setTenant(MultiTenantContext.getTenant());
            task.setImportData(newDatatTable);
        });
        datafeedDao.update(datafeed);
    }

}
