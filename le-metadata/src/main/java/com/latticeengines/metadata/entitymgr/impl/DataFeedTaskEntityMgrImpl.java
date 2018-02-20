package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.DataFeedTaskDao;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;

@Component("datafeedTaskEntityMgr")
public class DataFeedTaskEntityMgrImpl extends BaseEntityMgrImpl<DataFeedTask> implements DataFeedTaskEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskEntityMgrImpl.class);

    @Autowired
    private DataFeedTaskDao datafeedTaskDao;

    @Autowired
    private DataFeedTaskTableDao datafeedTaskTableDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Autowired
    private AttributeDao attributeDao;

    @Autowired
    private LastModifiedKeyDao lastModifiedKeyDao;

    @Autowired
    private PrimaryKeyDao primaryKeyDao;

    @Override
    public BaseDao<DataFeedTask> getDao() {
        return datafeedTaskDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void create(DataFeedTask datafeedTask) {
        Table templateTable = datafeedTask.getImportTemplate();
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        if (templateTable != null) {
            Table existing = tableEntityMgr.findByName(templateTable.getName());
            if (existing == null) {
                templateTable.setTableType(TableType.IMPORTTABLE);
                tableEntityMgr.create(templateTable);
            } else {
                datafeedTask.setImportTemplate(existing);
            }
        }
        tableTypeHolder.setTableType(TableType.DATATABLE);
        datafeedTaskDao.create(datafeedTask);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public Table peekFirstDataTable(DataFeedTask datafeedTask) {
        Table table = datafeedTaskTableDao.peekFirstDataTable(datafeedTask);
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public Table pollFirstDataTable(DataFeedTask datafeedTask) {
        Table table = datafeedTaskTableDao.pollFirstDataTable(datafeedTask);
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int getDataTableSize(DataFeedTask datafeedTask) {
        if (datafeedTask == null) {
            return 0;
        }
        return datafeedTaskTableDao.getDataFeedTaskTables(datafeedTask).size();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataFeedTaskTable> getDataTables(DataFeedTask datafeedTask) {
        if (datafeedTask == null) {
            return Collections.emptyList();
        }
        return datafeedTaskTableDao.getDataFeedTaskTables(datafeedTask);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void addTableToQueue(DataFeedTask datafeedTask, Table table) {
        if (table.getPid() == null) {
            table.setTableType(TableType.DATATABLE);
            tableEntityMgr.create(table);
        }
        if (!TableType.DATATABLE.equals(table.getTableType())) {
            throw new IllegalArgumentException(
                    "Can only put data table in the queue. But " + table.getName() + " is a " + table.getTableType());
        }
        DataFeedTaskTable datafeedTaskTable = new DataFeedTaskTable();
        datafeedTaskTable.setFeedTask(datafeedTask);
        datafeedTaskTable.setTable(table);
        datafeedTaskTableDao.create(datafeedTaskTable);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void clearTableQueue() {
        datafeedTaskTableDao.deleteAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void clearTableQueuePerTask(DataFeedTask dataFeedTask) {
        datafeedTaskTableDao.deleteDataFeedTaskTables(dataFeedTask);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void registerExtract(DataFeedTask datafeedTask, String tableName, Extract extract) {
        Table templateTable = getTemplate(tableName);
        registerExtractTable(templateTable, extract, datafeedTask);
        updateDataFeedTaskAfterRegister(datafeedTask);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void registerExtracts(DataFeedTask datafeedTask, String tableName, List<Extract> extracts) {
        Table templateTable = getTemplate(tableName);
        for (int i = 0; i < extracts.size(); i++) {
            Extract extract = extracts.get(i);
            registerExtractTable(templateTable, extract, datafeedTask);
        }
        updateDataFeedTaskAfterRegister(datafeedTask);
    }

    private Table getTemplate(String tableName) {
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        Table templateTable = tableEntityMgr.findByName(tableName);
        tableTypeHolder.setTableType(TableType.DATATABLE);
        templateTable.getExtracts().clear();
        return templateTable;
    }

    private void registerExtractTable(Table template, Extract extract, DataFeedTask datafeedTask) {
        Table cloneTable = TableUtils.clone(template, NamingUtils.uuid("DataTable"));
        cloneTable.setTenant(MultiTenantContext.getTenant());
        cloneTable.addExtract(extract);
        log.info(String.format("Adding extract to new data table %s", template.getName()));
        addTableToQueue(datafeedTask, cloneTable);
    }

    private void updateDataFeedTaskAfterRegister(DataFeedTask datafeedTask) {
        datafeedTask.setLastImported(new Date());
        boolean templateTableChanged = Status.Updated.equals(datafeedTask.getStatus());
        if (templateTableChanged) {
            datafeedTask.setStatus(Status.Active);
            log.info(String.format("Since import table has been updated, set data feed task to Active status %s",
                    datafeedTask));
        }
        update(datafeedTask, datafeedTask.getStatus(), datafeedTask.getLastImported());
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeedTask findByKey(DataFeedTask task) {
        DataFeedTask datafeedTask = datafeedTaskDao.findByKey(task);
        TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
        TableEntityMgr.inflateTable(datafeedTask.getImportData());
        return datafeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeedId) {
        DataFeedTask dataFeedTask = datafeedTaskDao.getDataFeedTask(source, dataFeedType, entity, dataFeedId);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public DataFeedTask getDataFeedTask(Long pid) {
        DataFeedTask dataFeedTask = datafeedTaskDao.findByField("PID", pid);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public DataFeedTask getDataFeedTask(String uniqueId) {
        DataFeedTask dataFeedTask = datafeedTaskDao.findByField("UNIQUE_ID", uniqueId);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String entity, Long dataFeedId) {
        List<DataFeedTask> dataFeedTasks = datafeedTaskDao.getDataFeedTaskWithSameEntity(entity, dataFeedId);
        if (dataFeedTasks != null) {
            for (DataFeedTask dataFeedTask : dataFeedTasks) {
                TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
                TableEntityMgr.inflateTable(dataFeedTask.getImportData());
            }
        }
        return dataFeedTasks;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteByTaskId(Long taskId) {
        DataFeedTask dataFeedTask = datafeedTaskDao.findByField("PID", taskId);
        if (dataFeedTask != null) {
            datafeedTaskDao.delete(dataFeedTask);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateDataFeedTask(DataFeedTask dataFeedTask) {
        DataFeedTask task = datafeedTaskDao.findByKey(dataFeedTask);
        TableEntityMgr.inflateTable(task.getImportTemplate());
        TableEntityMgr.inflateTable(task.getImportData());
        task.setLastImported(dataFeedTask.getLastImported());
        task.setActiveJob(dataFeedTask.getActiveJob());
        task.setStartTime(dataFeedTask.getStartTime());
        task.setSourceConfig(dataFeedTask.getSourceConfig());
        task.setStatus(dataFeedTask.getStatus());
        deleteReferences(task.getImportTemplate());
        task.getImportTemplate().setAttributes(dataFeedTask.getImportTemplate().getAttributes());
        updateReferences(task.getImportTemplate());
        createReferences(task.getImportTemplate());
        update(task);
    }

    private void deleteReferences(Table table) {
        if (table.getAttributes() != null) {
            for (Attribute attr : table.getAttributes()) {
                attributeDao.delete(attr);
            }
        }
    }

    private void updateReferences(Table table) {
        if (table.getPrimaryKey() != null) {
            table.getPrimaryKey().setTable(table);
        }
        if (table.getLastModifiedKey() != null) {
            table.getLastModifiedKey().setTable(table);
        }

        if (table.getExtracts() != null) {
            for (Extract extract : table.getExtracts()) {
                extract.setTable(table);
                extract.setTenant(table.getTenant());
            }
        }

        if (table.getAttributes() != null) {
            for (Attribute attr : table.getAttributes()) {
                attr.setTable(table);
                attr.setTenant(table.getTenant());
            }
        }
    }

    private void createReferences(Table table) {
        if (table.getPrimaryKey() != null) {
            primaryKeyDao.create(table.getPrimaryKey());
        }
        if (table.getLastModifiedKey() != null) {
            lastModifiedKeyDao.create(table.getLastModifiedKey());
        }

        if (table.getAttributes() != null) {
            for (Attribute attr : table.getAttributes()) {
                attributeDao.create(attr);
            }
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void update(DataFeedTask datafeedTask, Date startTime) {
        datafeedTaskDao.update(datafeedTask, startTime);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void update(DataFeedTask datafeedTask, Status status, Date lastImported) {
        datafeedTaskDao.update(datafeedTask, status, lastImported);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public List<Extract> getExtractsPendingInQueue(DataFeedTask task) {
        List<DataFeedTaskTable> datafeedTaskTables = getDataTables(task);
        List<Extract> extracts = new ArrayList<>();
        datafeedTaskTables.stream().map(DataFeedTaskTable::getTable).forEach(t -> {
            TableEntityMgr.inflateTable(t);
            extracts.addAll(t.getExtracts());
        });
        return extracts;
    }
}
