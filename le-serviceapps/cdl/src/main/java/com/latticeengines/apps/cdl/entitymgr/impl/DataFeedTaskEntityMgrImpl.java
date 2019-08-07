package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataFeedTaskDao;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskTableEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataFeedTaskRepository;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.dao.LastModifiedKeyDao;
import com.latticeengines.metadata.dao.PrimaryKeyDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;

@Component("datafeedTaskEntityMgr")
public class DataFeedTaskEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataFeedTask, Long>
        implements DataFeedTaskEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskEntityMgrImpl.class);

    @Inject
    private DataFeedTaskRepository datafeedTaskRepository;

    @Inject
    private DataFeedTaskDao datafeedTaskDao;

    @Inject
    private DataFeedTaskTableEntityMgr datafeedTaskTableEntityMgr;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Inject
    private TableTypeHolder tableTypeHolder;

    @Inject
    private AttributeDao attributeDao;

    @Inject
    private LastModifiedKeyDao lastModifiedKeyDao;

    @Autowired
    private PrimaryKeyDao primaryKeyDao;

    @Override
    public BaseDao<DataFeedTask> getDao() {
        return datafeedTaskDao;
    }

    @Override
    public BaseJpaRepository<DataFeedTask, Long> getRepository() {
        return datafeedTaskRepository;
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
        datafeedTaskTableEntityMgr.create(datafeedTaskTable);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void addTableToQueue(String dataFeedTaskUniqueId, String tableName) {
        if (StringUtils.isEmpty(dataFeedTaskUniqueId) || StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("DataFeedTask Unique Id & table name cannot be null");
        }
        DataFeedTask dataFeedTask = getDataFeedTask(dataFeedTaskUniqueId);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find data feed task with unique id: " + dataFeedTaskUniqueId);
        }
        Table table = tableEntityMgr.findByName(tableName);
        if (table == null) {
            throw new RuntimeException("Cannot find table with name: " + tableName);
        }
        addTableToQueue(dataFeedTask, table);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void addTablesToQueue(String dataFeedTaskUniqueId, List<String> tableNames) {
        if (StringUtils.isEmpty(dataFeedTaskUniqueId) || tableNames == null || tableNames.size() == 0) {
            throw new IllegalArgumentException("DataFeedTask Unique Id & table name cannot be null");
        }
        DataFeedTask dataFeedTask = getDataFeedTask(dataFeedTaskUniqueId);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find data feed task with unique id: " + dataFeedTaskUniqueId);
        }
        for (String tableName : tableNames) {
            Table table = tableEntityMgr.findByName(tableName);
            if (table == null) {
                log.error("Cannot find table with name: " + tableName);
                continue;
            }
            addTableToQueue(dataFeedTask, table);
        }
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    private void createDataTable(Table table) {
        if (table.getPid() == null) {
            table.setTableType(TableType.DATATABLE);
            tableEntityMgr.create(table);
        }
        if (!TableType.DATATABLE.equals(table.getTableType())) {
            throw new IllegalArgumentException(
                    "Can only put data table in the queue. But " + table.getName() + " is a " + table.getTableType());
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void clearTableQueue() {
        datafeedTaskTableEntityMgr.deleteAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void clearTableQueuePerTask(DataFeedTask dataFeedTask) {
        datafeedTaskTableEntityMgr.deleteDataFeedTaskTables(dataFeedTask);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public List<String> registerExtract(DataFeedTask datafeedTask, String tableName, Extract extract) {
        Table templateTable = getTemplate(tableName);
        String registeredTableName = registerExtractTable(templateTable, extract, datafeedTask);
        updateDataFeedTaskAfterRegister(datafeedTask);
        return Collections.singletonList(registeredTableName);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public List<String> registerExtracts(DataFeedTask datafeedTask, String tableName, List<Extract> extracts) {
        Table templateTable = getTemplate(tableName);
        List<String> registeredTables = new ArrayList<>();
        for (int i = 0; i < extracts.size(); i++) {
            Extract extract = extracts.get(i);
            registeredTables.add(registerExtractTable(templateTable, extract, datafeedTask));
        }
        updateDataFeedTaskAfterRegister(datafeedTask);
        return registeredTables;
    }

    private Table getTemplate(String tableName) {
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        Table templateTable = tableEntityMgr.findByName(tableName);
        tableTypeHolder.setTableType(TableType.DATATABLE);
        templateTable.getExtracts().clear();
        return templateTable;
    }

    private String registerExtractTable(Table template, Extract extract, DataFeedTask datafeedTask) {
        Table cloneTable = TableUtils.clone(template, NamingUtils.uuid("DataTable"));
        cloneTable.setTenant(MultiTenantContext.getTenant());
        cloneTable.addExtract(extract);
        log.info(String.format("Adding extract to new data table %s", template.getName()));
        createDataTable(cloneTable);
        return cloneTable.getName();
    }

    private void updateDataFeedTaskAfterRegister(DataFeedTask datafeedTask) {
        datafeedTask.setLastImported(new Date());
        datafeedTask.setLastUpdated(new Date());
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
        DataFeedTask datafeedTask = datafeedTaskRepository.findById(task.getPid()).orElse(null);
        if (datafeedTask != null) {
            TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(datafeedTask.getImportData());
        }
        return datafeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, DataFeed datafeed) {
        DataFeedTask dataFeedTask = datafeedTaskRepository.findBySourceAndFeedTypeAndEntityAndDataFeed(source,
                dataFeedType, entity, datafeed);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public DataFeedTask getDataFeedTask(String source, String dataFeedType, DataFeed datafeed) {
        DataFeedTask dataFeedTask = datafeedTaskRepository.findBySourceAndFeedTypeAndDataFeed(source, dataFeedType,
                datafeed);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public DataFeedTask getDataFeedTask(Long pid) {
        DataFeedTask dataFeedTask = datafeedTaskRepository.findById(pid).orElse(null);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public DataFeedTask getDataFeedTask(String uniqueId) {
        DataFeedTask dataFeedTask = datafeedTaskRepository.findByUniqueId(uniqueId);
        if (dataFeedTask != null) {
            TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(dataFeedTask.getImportData());
        }
        return dataFeedTask;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String entity, DataFeed datafeed) {
        List<DataFeedTask> dataFeedTasks = datafeedTaskRepository.findByEntityAndDataFeed(entity, datafeed);
        if (dataFeedTasks != null) {
            for (DataFeedTask dataFeedTask : dataFeedTasks) {
                TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
                TableEntityMgr.inflateTable(dataFeedTask.getImportData());
            }
        }
        return dataFeedTasks;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_COMMITTED)
    public List<DataFeedTask> getDataFeedTaskByUniqueIds(List<String> uniqueIds) {
        List<DataFeedTask> dataFeedTasks = datafeedTaskRepository.findByUniqueIdIn(uniqueIds);
        if (CollectionUtils.isNotEmpty(dataFeedTasks)) {
            dataFeedTasks.forEach(dataFeedTask -> {
                TableEntityMgr.inflateTable(dataFeedTask.getImportTemplate());
                TableEntityMgr.inflateTable(dataFeedTask.getImportData());});
        }
        return dataFeedTasks;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void deleteByTaskId(Long taskId) {
        DataFeedTask datafeedTask = datafeedTaskRepository.findById(taskId).orElse(null);
        if (datafeedTask != null) {
            delete(datafeedTask);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateDataFeedTask(DataFeedTask dataFeedTask) {
        DataFeedTask task = datafeedTaskRepository.findById(dataFeedTask.getPid()).get();
        TableEntityMgr.inflateTable(task.getImportTemplate());
        TableEntityMgr.inflateTable(task.getImportData());
        task.setLastImported(dataFeedTask.getLastImported());
        task.setLastUpdated(new Date());
        task.setActiveJob(dataFeedTask.getActiveJob());
        task.setStartTime(dataFeedTask.getStartTime());
        task.setSourceConfig(dataFeedTask.getSourceConfig());
        task.setStatus(dataFeedTask.getStatus());
        task.setS3ImportStatus(dataFeedTask.getS3ImportStatus());
        deleteReferences(task.getImportTemplate());
        task.setTemplateDisplayName(dataFeedTask.getTemplateDisplayName());
        task.setFeedType(dataFeedTask.getFeedType());
        task.getImportTemplate().setAttributes(dataFeedTask.getImportTemplate().getAttributes());
        updateReferences(task.getImportTemplate());
        createReferences(task.getImportTemplate());
        datafeedTaskDao.update(task);
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
            primaryKeyDao.create(primaryKeyDao.merge(table.getPrimaryKey()));
        }
        if (table.getLastModifiedKey() != null) {
            lastModifiedKeyDao.create(lastModifiedKeyDao.merge(table.getLastModifiedKey()));
        }

        if (table.getAttributes() != null) {
            for (Attribute attr : table.getAttributes()) {
                attributeDao.create(attributeDao.merge(attr));
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
        List<DataFeedTaskTable> datafeedTaskTables = datafeedTaskTableEntityMgr.getInflatedDataFeedTaskTables(task);
        List<Extract> extracts = new ArrayList<>();
        datafeedTaskTables.stream().map(DataFeedTaskTable::getTable).forEach(t -> {
            extracts.addAll(t.getExtracts());
        });
        return extracts;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public List<DataFeedTaskTable> getInflatedDataFeedTaskTables(DataFeedTask task) {
        List<DataFeedTaskTable> datafeedTaskTables = datafeedTaskTableEntityMgr.getInflatedDataFeedTaskTables(task);
        List<DataFeedTaskTable> inflatedDatafeedTaskTables = new ArrayList<>();
        datafeedTaskTables.stream().forEach(datafeedtaskTable -> {
            Table dataTable = datafeedtaskTable.getTable();
            if (dataTable != null) {
                if (!dataTable.getExtracts().isEmpty()) {
                    inflatedDatafeedTaskTables.add(datafeedtaskTable);
                } else {
                    log.info(String.format("skip table: %s as this table extract is empty", dataTable.getName()));
                }
            }
        });
        return inflatedDatafeedTaskTables;
    }
}
