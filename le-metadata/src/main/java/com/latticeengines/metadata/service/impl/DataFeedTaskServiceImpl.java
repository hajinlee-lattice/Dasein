package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.metadata.entitymgr.DataFeedEntityMgr;
import com.latticeengines.metadata.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.metadata.service.DataFeedTaskService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataFeedTaskService")
public class DataFeedTaskServiceImpl implements DataFeedTaskService {

    @Autowired
    private DataFeedTaskEntityMgr dataFeedTaskEntityMgr;

    @Autowired
    private DataFeedEntityMgr dataFeedEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    // @Autowired
    // private MetadataService mdService;

    @Override
    public void createDataFeedTask(String customerSpace, String dataFeedName, DataFeedTask dataFeedTask) {
        // Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        // MultiTenantContext.setTenant(tenant);
        DataFeed dataFeed = dataFeedEntityMgr.findByName(dataFeedName);
        dataFeedTask.setDataFeed(dataFeed);
        // dataFeedTask.getImportTemplate().setTableType(TableType.IMPORTTABLE);
        // dataFeedTask.getImportTemplate().setTenant(tenant);
        // mdService.createTable(CustomerSpace.parse(customerSpace),
        // dataFeedTask.getImportTemplate());
        // dataFeedTask.setImportTemplate(mdService.getImportTable(CustomerSpace.parse(customerSpace),
        // dataFeedTask
        // .getImportTemplate().getName()));
        dataFeedTaskEntityMgr.create(dataFeedTask);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity,
            String dataFeedName) {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));
        DataFeed dataFeed = dataFeedEntityMgr.findByNameInflated(dataFeedName);
        if (dataFeed == null) {
            return null;
        }
        // return dataFeed.getTask(entity, source, dataFeedType);
        return dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, entity, dataFeed.getPid());
    }

    @Override
    public DataFeedTask getDataFeedTask(Long taskId) {
        return dataFeedTaskEntityMgr.getDataFeedTask(taskId);
    }

    @Override
    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        // Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        // MultiTenantContext.setTenant(tenant);
        //
        // DatabaseUtils.retry("updateDataFeedTask", new Closure() {
        // @Override
        // public void execute(Object input) {
        // DataFeedTask found = dataFeedTaskEntityMgr.findByKey(dataFeedTask);
        // if (found != null) {
        // dataFeedTaskEntityMgr.delete(found);
        // }
        // dataFeedTask.getImportTemplate().setTableType(TableType.IMPORTTABLE);
        // dataFeedTask.getImportTemplate().setTenant(tenant);
        // dataFeedTaskEntityMgr.createDataFeedTask(dataFeedTask);
        // }
        // });
        // Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        // MultiTenantContext.setTenant(tenant);
        // dataFeedTask.getImportTemplate().setTableType(TableType.IMPORTTABLE);
        // dataFeedTask.getImportTemplate().setTenant(tenant);
        // mdService.updateTable(CustomerSpace.parse(customerSpace),
        // dataFeedTask.getImportTemplate());
        dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask);
    }

    @Override
    public void registerExtract(String customerSpace, Long taskId, Extract extract) {
        DataFeedTask dataFeedTask = getDataFeedTask(taskId);
        dataFeedTaskEntityMgr.registerExtract(dataFeedTask, extract);
    }
}
