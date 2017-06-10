package com.latticeengines.metadata.entitymgr.impl;

import java.util.Date;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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
import com.latticeengines.metadata.dao.ExtractDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
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

    @Autowired
    private AttributeDao attributeDao;

    @Autowired
    private ExtractDao extractDao;

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
        if (dataFeedTask.getImportTemplate() != null) {
            dataFeedTask.getImportTemplate().setTableType(TableType.IMPORTTABLE);
            tableEntityMgr.create(dataFeedTask.getImportTemplate());
        }
        if (dataFeedTask.getImportData() != null) {
            dataFeedTask.getImportData().setTableType(TableType.DATATABLE);
            tableEntityMgr.create(dataFeedTask.getImportData());
        }
        super.create(dataFeedTask);
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
            if (dataFeedTask.getImportData().getPid() == null) {
                dataFeedTask.getImportData().setTableType(TableType.DATATABLE);
                tableEntityMgr.create(dataFeedTask.getImportData());
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
            newDataTable.setTableType(TableType.DATATABLE);
            tableEntityMgr.create(newDataTable);
            dataFeedTask.setImportData(newDataTable);
            dataFeedTask.setStatus(Status.Active);
            dataFeedTask.setLastImported(new Date());
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
    // @Override
    // @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    // public boolean dataFeedTaskExist(String dataFeedType, String entity) {
    // DataFeedTask dataFeedTask = datafeedTaskDao.getDataFeedTask(dataFeedType,
    // entity);
    // return dataFeedTask != null;
    // }

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
        // tableEntityMgr.create(dataFeedTask.getImportTemplate());
        // dataFeedTask.setImportTemplate(tableEntityMgr.findByName(dataFeedTask.getImportTemplate().getName()));
        // create(dataFeedTask);
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

        datafeedTaskDao.update(task);
        // createOrUpdate(dataFeedTask);
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

        if (table.getExtracts() != null) {
            for (Extract extract : table.getExtracts()) {
                extractDao.create(extract);
            }
        }

        if (table.getAttributes() != null) {
            for (Attribute attr : table.getAttributes()) {
                attributeDao.create(attr);
            }
        }
    }
}
