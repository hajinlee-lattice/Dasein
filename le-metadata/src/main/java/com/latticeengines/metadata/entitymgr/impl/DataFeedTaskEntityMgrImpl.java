package com.latticeengines.metadata.entitymgr.impl;

import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.DataFeedTask.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedTaskTable;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.DataFeedTaskDao;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("datafeedTaskEntityMgr")
public class DataFeedTaskEntityMgrImpl extends BaseEntityMgrImpl<DataFeedTask> implements DataFeedTaskEntityMgr {

    private static final Logger log = Logger.getLogger(DataFeedTaskEntityMgrImpl.class);

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
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataFeedTask dataFeedTask) {
        Table dataTable = dataFeedTask.getImportData();
        Table templateTable = dataFeedTask.getImportTemplate();
        if (dataTable != null) {
            Table existing = tableEntityMgr.findByName(dataTable.getName());
            if (existing == null) {
                dataTable.setTableType(TableType.DATATABLE);
                tableEntityMgr.create(dataTable);
            } else {
                dataFeedTask.setImportData(existing);
            }
        }
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        if (templateTable != null) {
            Table existing = tableEntityMgr.findByName(templateTable.getName());
            if (existing == null) {
                templateTable.setTableType(TableType.IMPORTTABLE);
                tableEntityMgr.create(templateTable);
            } else {
                dataFeedTask.setImportTemplate(existing);
            }
        }
        tableTypeHolder.setTableType(TableType.DATATABLE);
        datafeedTaskDao.create(dataFeedTask);
        addImportDataTableToQueue(dataFeedTask);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Table peekFirstDataTable(DataFeedTask dataFeedTask) {
        Table table = datafeedTaskTableDao.peekFirstDataTable(dataFeedTask);
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Table pollFirstDataTable(DataFeedTask dataFeedTask) {
        Table table = datafeedTaskTableDao.pollFirstDataTable(dataFeedTask);
        TableEntityMgr.inflateTable(table);
        return table;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int getDataTableSize(DataFeedTask dataFeedTask) {
        if (dataFeedTask == null) {
            return 0;
        }
        return datafeedTaskTableDao.getDataTableSize(dataFeedTask);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addImportDataTableToQueue(DataFeedTask dataFeedTask) {
        try {
            if (dataFeedTask.getImportData() != null) {
                if (dataFeedTask.getImportData().getPid() == null) {
                    dataFeedTask.getImportData().setTableType(TableType.DATATABLE);
                    tableEntityMgr.create(dataFeedTask.getImportData());
                    createOrUpdate(dataFeedTask);
                }
                addTableToQueue(dataFeedTask, dataFeedTask.getImportData());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addTableToQueue(DataFeedTask dataFeedTask, Table table) {
        if (!TableType.DATATABLE.equals(table.getTableType())) {
            throw new IllegalArgumentException(
                    "Can only put data table in the queue. But " + table.getName() + " is a " + table.getTableType());
        }
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
    public void registerExtract(DataFeedTask datafeedTask, String tableName, Extract extract) {
        boolean templateTableChanged = datafeedTask.getStatus() == Status.Updated;
        boolean dataTableConsumed = datafeedTask.getImportData() == null;

        if (!dataTableConsumed) {
            log.info(String.format("directly appending extract to data table %s",
                    datafeedTask.getImportData().getName()));
            tableEntityMgr.addExtract(datafeedTask.getImportData(), extract);

        } else {
            tableTypeHolder.setTableType(TableType.IMPORTTABLE);
            Table extractTable = tableEntityMgr.findByName(tableName);
            extractTable.getExtracts().clear();
            extractTable = TableUtils.clone(extractTable, NamingUtils.uuid("DataTable"));
            extractTable.setTenant(MultiTenantContext.getTenant());
            extractTable.addExtract(extract);
            extractTable.setTableType(TableType.DATATABLE);
            tableTypeHolder.setTableType(TableType.DATATABLE);
            log.info(String.format("data table has been consumed, adding extract to new data table %s",
                    extractTable.getName()));
            tableEntityMgr.create(extractTable);
            addTableToQueue(datafeedTask, extractTable);
        }
        if (templateTableChanged || dataTableConsumed) {
            Table newDataTable = TableUtils.clone(datafeedTask.getImportTemplate(), //
                    NamingUtils.uuid("DataTable"));
            newDataTable.setTenant(MultiTenantContext.getTenant());
            newDataTable.setTableType(TableType.DATATABLE);
            tableEntityMgr.create(newDataTable);
            datafeedTask.setImportData(newDataTable);
            datafeedTask.setStatus(Status.Active);
            datafeedTask.setLastImported(new Date());
            log.info(String.format("creating new import table for data feed task %s", datafeedTask));
            createOrUpdate(datafeedTask);
            addTableToQueue(datafeedTask, datafeedTask.getImportData());
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

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createDataFeedTask(DataFeedTask dataFeedTask) {
        create(dataFeedTask);
        Table importTemplate = dataFeedTask.getImportTemplate();
        updateReferences(importTemplate);
        createReferences(importTemplate);
        Table importData = dataFeedTask.getImportData();
        if (importData != null) {
            updateReferences(importData);
            createReferences(importData);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeedId) {
        DataFeedTask dataFeedTask = datafeedTaskDao.getDataFeedTask(source, dataFeedType, entity, dataFeedId);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataFeedTask getDataFeedTask(Long pid) {
        DataFeedTask dataFeedTask = datafeedTaskDao.findByField("PID", pid);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByTaskId(Long taskId) {
        DataFeedTask dataFeedTask = datafeedTaskDao.findByField("PID", taskId);
        if (dataFeedTask != null) {
            datafeedTaskDao.delete(dataFeedTask);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
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
}
