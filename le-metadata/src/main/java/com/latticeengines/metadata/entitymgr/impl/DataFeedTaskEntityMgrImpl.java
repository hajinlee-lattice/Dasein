package com.latticeengines.metadata.entitymgr.impl;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.DataFeedTask.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedTaskTable;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.DataFeedTaskDao;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("datafeedTaskEntityMgr")
public class DataFeedTaskEntityMgrImpl extends BaseEntityMgrImpl<DataFeedTask> implements DataFeedTaskEntityMgr {

    @Autowired
    private DataFeedTaskDao datafeedTaskDao;

    @Autowired
    private DataFeedTaskTableDao datafeedTaskTableDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Override
    public BaseDao<DataFeedTask> getDao() {
        return datafeedTaskDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table peekFirstDataTable(Long taskPid) {
        Table table = datafeedTaskTableDao.peekFirstDataTable(taskPid);
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Table pollFirstDataTable(Long taskPid) {
        Table table = datafeedTaskTableDao.pollFirstDataTable(taskPid);
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int getDataTableSize(Long taskPid) {
        DataFeedTask task = findByField("pid", taskPid);
        if (task == null) {
            return 0;
        }
        HibernateUtils.inflateDetails(task);
        return task.getTables().size();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addImportDataTableToQueue(DataFeedTask dataFeedTask) {
        try {
            if (dataFeedTask.getImportData().getPid() == null) {
                createOrUpdate(dataFeedTask);
            }
            addTableToQueue(dataFeedTask, dataFeedTask.getImportData());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addTableToQueue(DataFeedTask dataFeedTask, Table table) {
        DataFeedTaskTable datafeedTaskTable = new DataFeedTaskTable();
        datafeedTaskTable.setFeedTask(dataFeedTask);
        datafeedTaskTable.setTable(table);
        datafeedTaskTableDao.create(datafeedTaskTable);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void clearTableQueue() {
        datafeedTaskTableDao.deleteAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void registerExtract(DataFeedTask dataFeedTask, Extract extract) {
        boolean templateTableChanged = dataFeedTask.getStatus() == Status.Updated;
        boolean dataTableConsumed = dataFeedTask.getImportData() == null;

        if (!dataTableConsumed) {
            tableEntityMgr.addExtract(dataFeedTask.getImportData(), extract);
        } else {
            tableTypeHolder.setTableType(TableType.IMPORTTABLE);
            Table extractTable = tableEntityMgr.findByName(extract.getTable().getName());
            extractTable.getExtracts().clear();
            extractTable = TableUtils.clone(extractTable,
                    "datatable_" + UUID.randomUUID().toString().replace('-', '_'));
            extractTable.setTenant(MultiTenantContext.getTenant());
            extractTable.addExtract(extract);
            tableTypeHolder.setTableType(TableType.DATATABLE);
            tableEntityMgr.create(extractTable);
            addTableToQueue(dataFeedTask, extractTable);
        }
        if (templateTableChanged || dataTableConsumed) {
            Table newDataTable = TableUtils.clone(dataFeedTask.getImportTemplate(),
                    "datatable_" + UUID.randomUUID().toString().replace('-', '_'));
            newDataTable.setTenant(MultiTenantContext.getTenant());
            dataFeedTask.setImportData(newDataTable);
            dataFeedTask.setStatus(Status.Active);
            createOrUpdate(dataFeedTask);
            addTableToQueue(dataFeedTask, dataFeedTask.getImportData());
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeedTask findByKey(DataFeedTask task) {
        DataFeedTask datafeedTask = datafeedTaskDao.findByKey(task);
        TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
        TableEntityMgr.inflateTable(datafeedTask.getImportData());
        return datafeedTask;
    }
}
