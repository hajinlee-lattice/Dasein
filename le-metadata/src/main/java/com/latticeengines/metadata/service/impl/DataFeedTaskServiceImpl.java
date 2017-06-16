package com.latticeengines.metadata.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.metadata.service.DataFeedTaskService;

@Component("dataFeedTaskService")
public class DataFeedTaskServiceImpl implements DataFeedTaskService {

    private static final Log log = LogFactory.getLog(DataFeedTaskServiceImpl.class);

    @Autowired
    private DataFeedTaskEntityMgr dataFeedTaskEntityMgr;

    @Autowired
    private DataFeedEntityMgr dataFeedEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Override
    public void createDataFeedTask(String customerSpace, String dataFeedName, DataFeedTask dataFeedTask) {
        if (dataFeedTask.getImportData() != null) {
            String tableName = dataFeedTask.getImportData().getName();
            Table exiting = tableEntityMgr.findByName(tableName);
            if (exiting != null) {
                log.warn("The import data table " + tableName + " in data feed task already exist. Delete it first.");
                tableEntityMgr.deleteByName(tableName);
            }
            dataFeedTask.getImportData().setTableType(TableType.DATATABLE);
            log.info("Creating import template table " + tableName + ".");
            tableEntityMgr.create(dataFeedTask.getImportData());
        }

        if (dataFeedTask.getImportTemplate() != null) {
            tableTypeHolder.setTableType(TableType.IMPORTTABLE);
            String tableName = dataFeedTask.getImportTemplate().getName();
            Table exiting = tableEntityMgr.findByName(tableName);
            if (exiting != null) {
                log.warn("The import template table " + tableName
                        + " in data feed task already exist. Delete it first.");
                tableEntityMgr.deleteByName(tableName);
            }
            dataFeedTask.getImportTemplate().setTableType(TableType.IMPORTTABLE);
            log.info("Creating import template table " + tableName + ".");
            tableEntityMgr.create(dataFeedTask.getImportTemplate());
            tableTypeHolder.setTableType(TableType.DATATABLE);
        }

        DataFeed dataFeed = dataFeedEntityMgr.findByName(dataFeedName);
        dataFeedTask.setDataFeed(dataFeed);
        dataFeedTaskEntityMgr.create(dataFeedTask);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity,
            String dataFeedName) {
        DataFeed dataFeed = dataFeedEntityMgr.findByNameInflated(dataFeedName);
        if (dataFeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, entity, dataFeed.getPid());
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, Long taskId) {
        return dataFeedTaskEntityMgr.getDataFeedTask(taskId);
    }

    @Override
    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask);
    }

    @Override
    public void registerExtract(String customerSpace, Long taskId, String tableName, Extract extract) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskId);
        dataFeedTaskEntityMgr.registerExtract(dataFeedTask, tableName, extract);
    }
}
