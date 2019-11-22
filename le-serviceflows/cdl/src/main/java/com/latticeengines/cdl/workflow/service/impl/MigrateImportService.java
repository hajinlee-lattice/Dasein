package com.latticeengines.cdl.workflow.service.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateImportServiceConfiguration;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("migrateImportService")
@Lazy(value = false)
public class MigrateImportService
        extends ConvertBatchStoreService<MigrateImportServiceConfiguration> {

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public String getOutputDataFeedTaskId(String customerSpace, MigrateImportServiceConfiguration config) {
        ImportMigrateTracking migrateTracking = getImportMigrateTracking(customerSpace, config);
        switch (config.getEntity()) {
            case Account:
                return migrateTracking.getReport().getOutputAccountTaskId();
            case Contact:
                return migrateTracking.getReport().getOutputContactTaskId();
            case Transaction:
                return migrateTracking.getReport().getOutputTransactionTaskId();
            default:
                throw new IllegalArgumentException("Not supported entity: " + config.getEntity().name());
        }
    }

    private ImportMigrateTracking getImportMigrateTracking(String customerSpace, MigrateImportServiceConfiguration config) {
        ImportMigrateTracking migrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace,
                config.getMigrateTrackingPid());
        if (migrateTracking == null || migrateTracking.getReport() == null) {
            throw new RuntimeException("Migrate Tracking Record is not correctly created!");
        }
        return migrateTracking;
    }

    @Override
    public Long getImportCounts(String customerSpace, MigrateImportServiceConfiguration config) {
        ImportMigrateTracking migrateTracking = getImportMigrateTracking(customerSpace, config);
        switch (config.getEntity()) {
            case Account:
                return migrateTracking.getReport().getAccountCounts();
            case Contact:
                return migrateTracking.getReport().getContactCounts();
            case Transaction:
                return migrateTracking.getReport().getTransactionCounts();
            default:
                throw new IllegalArgumentException("Not supported entity: " + config.getEntity().name());
        }
    }

    @Override
    public List<String> getRegisteredDataTables(String customerSpace, MigrateImportServiceConfiguration config) {
        ImportMigrateTracking migrateTracking = getImportMigrateTracking(customerSpace, config);
        switch (config.getEntity()) {
            case Account:
                return migrateTracking.getReport().getAccountDataTables();
            case Contact:
                return migrateTracking.getReport().getContactDataTables();
            case Transaction:
                return migrateTracking.getReport().getTransactionDataTables();
            default:
                throw new IllegalArgumentException("Not supported entity: " + config.getEntity().name());
        }
    }

    @Override
    public Map<String, String> getDuplicateMap(String customerSpace, MigrateImportServiceConfiguration config) {
        ImportMigrateTracking migrateTracking = getImportMigrateTracking(customerSpace, config);
        if (StringUtils.isEmpty(migrateTracking.getReport().getSystemName())) {
            throw new RuntimeException("Import System is not properly setup for migrate!");
        }
        String systemName = migrateTracking.getReport().getSystemName();
        S3ImportSystem importSystem = cdlProxy.getS3ImportSystem(customerSpace, systemName);
        Map<String, String> dupMap = new HashedMap<>();
        switch (config.getEntity()) {
            case Account:
                if (StringUtils.isNotEmpty(importSystem.getAccountSystemId())) {
                    dupMap.put(InterfaceName.AccountId.name(), importSystem.getAccountSystemId());
                }
                break;
            case Contact:
                if (StringUtils.isNotEmpty(importSystem.getContactSystemId())) {
                    dupMap.put(InterfaceName.ContactId.name(), importSystem.getContactSystemId());
                }
                break;
            case Transaction:
                break;
            default:
                throw new IllegalArgumentException("Not supported entity: " + config.getEntity().name());
        }
        return dupMap;
    }

    @Override
    public Map<String, String> getRenameMap(String customerSpace, MigrateImportServiceConfiguration config) {
        ImportMigrateTracking migrateTracking = getImportMigrateTracking(customerSpace, config);
        Map<String, String> renameMap = new HashedMap<>();
        String taskId;
        DataFeedTask dataFeedTask;
        switch (config.getEntity()) {
            case Account:
                taskId = migrateTracking.getReport().getOutputAccountTaskId();
                dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskId);
                if (dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CustomerAccountId) != null) {
                    renameMap.put(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name());
                }
                break;
            case Contact:
                taskId = migrateTracking.getReport().getOutputContactTaskId();
                dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskId);
                if (dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CustomerContactId) != null) {
                    renameMap.put(InterfaceName.ContactId.name(), InterfaceName.CustomerContactId.name());
                }
                if (dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CustomerAccountId) != null) {
                    renameMap.put(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name());
                }
                break;
            case Transaction:
                taskId = migrateTracking.getReport().getOutputTransactionTaskId();
                dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskId);
                if (dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CustomerAccountId) != null) {
                    renameMap.put(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name());
                }
                if (dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CustomerContactId) != null) {
                    renameMap.put(InterfaceName.ContactId.name(), InterfaceName.CustomerContactId.name());
                }
                break;
            default:
                throw new IllegalArgumentException("Not supported entity: " + config.getEntity().name());
        }
        return renameMap;
    }

    @Override
    public void updateConvertResult(String customerSpace, MigrateImportServiceConfiguration config, Long importCounts, List<String> dataTables) {
        ImportMigrateTracking migrateTracking = getImportMigrateTracking(customerSpace, config);
        switch (config.getEntity()) {
            case Account:
                migrateTracking.getReport().setAccountCounts(importCounts);
                migrateTracking.getReport().setAccountDataTables(dataTables);
                break;
            case Contact:
                migrateTracking.getReport().setContactCounts(importCounts);
                migrateTracking.getReport().setContactDataTables(dataTables);
                break;
            case Transaction:
                migrateTracking.getReport().setTransactionCounts(importCounts);
                migrateTracking.getReport().setTransactionDataTables(dataTables);
                break;
            default:
                throw new IllegalArgumentException("Not supported entity: " + config.getEntity().name());
        }
        migrateTrackingProxy.updateReport(customerSpace, migrateTracking.getPid(), migrateTracking.getReport());
    }

    @Override
    public void updateRegisteredAction(String customerSpace, MigrateImportServiceConfiguration config, Long actionId) {
        ImportMigrateTracking migrateTracking = getImportMigrateTracking(customerSpace, config);
        switch (config.getEntity()) {
            case Account:
                migrateTracking.getReport().setAccountActionId(actionId);
                break;
            case Contact:
                migrateTracking.getReport().setContactActionId(actionId);
                break;
            case Transaction:
                migrateTracking.getReport().setTransactionActionId(actionId);
                break;
            default:
                throw new IllegalArgumentException("Not supported entity: " + config.getEntity().name());
        }
        migrateTrackingProxy.updateReport(customerSpace, migrateTracking.getPid(), migrateTracking.getReport());
    }

    @Override
    public void setDataTable(String migratedImportTableName, String customerSpace, Table templateTable, MigrateImportServiceConfiguration config, Configuration yarnConfiguration) {
        Table migratedImportTable = metadataProxy.getTable(customerSpace, migratedImportTableName);
        Long importCounts = getTableDataLines(migratedImportTable, yarnConfiguration);
        List<String> dataTables = dataFeedProxy.registerExtracts(customerSpace, getOutputDataFeedTaskId(customerSpace
                , config), templateTable.getName(), migratedImportTable.getExtracts());
        updateConvertResult(customerSpace, config, importCounts,
                dataTables);
    }

    @Override
    public Table verifyTenantStatus(String customerSpace, MigrateImportServiceConfiguration config) {
        String taskUniqueId = getOutputDataFeedTaskId(customerSpace, config);
        if (StringUtils.isEmpty(taskUniqueId)) {
            throw new RuntimeException("Cannot find the target datafeed task for Account migrate!");
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskUniqueId);
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the dataFeedTask with id: " + taskUniqueId);
        }
        Table templateTable = dataFeedTask.getImportTemplate();
        if (templateTable == null) {
            throw new RuntimeException("Template is NULL for dataFeedTask: " + taskUniqueId);
        }
        return templateTable;
    }

    @Override
    public List<String> getAttributes(String customerSpace, Table templateTable, 
                                      Table masterTable, MigrateImportServiceConfiguration config) {
        return templateTable.getAttributes().stream()
                .map(Attribute::getName)
                .distinct()
                .filter(attrName -> masterTable.getAttribute(attrName) == null)
                .collect(Collectors.toList());
    }

    @Override
    public Table getMasterTable(String customerSpace,
                                TableRoleInCollection batchStore, MigrateImportServiceConfiguration config) {
        return dataCollectionProxy.getTable(customerSpace, batchStore);
    }
}
