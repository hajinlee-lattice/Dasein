package com.latticeengines.cdl.workflow.service.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreToImportServiceConfiguration;
import com.latticeengines.proxy.exposed.cdl.ConvertBatchStoreInfoProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("baseConvertToImportService")
@Lazy(value = false)
public class BaseConvertToImportService
        extends ConvertBatchStoreService<ConvertBatchStoreToImportServiceConfiguration> {

    @Inject
    private ConvertBatchStoreInfoProxy convertBatchStoreInfoProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public String getOutputDataFeedTaskId(String customerSpace, ConvertBatchStoreToImportServiceConfiguration config) {
        ConvertBatchStoreInfo convertInfo = getConvertInfo(customerSpace, config);
        return convertInfo.getConvertDetail().getTaskUniqueId();
    }

    @Override
    public Long getImportCounts(String customerSpace, ConvertBatchStoreToImportServiceConfiguration config) {
        ConvertBatchStoreInfo convertInfo = getConvertInfo(customerSpace, config);
        return convertInfo.getConvertDetail().getImportCount();
    }

    @Override
    public List<String> getRegisteredDataTables(String customerSpace, ConvertBatchStoreToImportServiceConfiguration config) {
        ConvertBatchStoreInfo convertInfo = getConvertInfo(customerSpace, config);
        return convertInfo.getConvertDetail().getDataTables();
    }

    @Override
    public Map<String, String> getDuplicateMap(String customerSpace, ConvertBatchStoreToImportServiceConfiguration config) {
        ConvertBatchStoreInfo convertInfo = getConvertInfo(customerSpace, config);
        return convertInfo.getConvertDetail().getDuplicateMap();
    }

    @Override
    public Map<String, String> getRenameMap(String customerSpace, ConvertBatchStoreToImportServiceConfiguration config) {
        ConvertBatchStoreInfo convertInfo = getConvertInfo(customerSpace, config);
        return convertInfo.getConvertDetail().getRenameMap();
    }

    @Override
    public void updateConvertResult(String customerSpace, ConvertBatchStoreToImportServiceConfiguration config, Long importCounts, List<String> dataTables) {
        ConvertBatchStoreInfo convertInfo = getConvertInfo(customerSpace, config);
        ConvertBatchStoreDetail detail = convertInfo.getConvertDetail();
        detail.setImportCount(importCounts);
        detail.setDataTables(dataTables);
        convertBatchStoreInfoProxy.updateDetail(customerSpace, convertInfo.getPid(), detail);
    }

    @Override
    public void updateRegisteredAction(String customerSpace, ConvertBatchStoreToImportServiceConfiguration config, Long actionId) {
        // do nothing.
    }

    @Override
    public void setDataTable(String migratedImportTableName, String customerSpace, Table templateTable,
                               ConvertBatchStoreToImportServiceConfiguration convertServiceConfig,
                             Configuration yarnConfiguration) {
        Table migratedImportTable = metadataProxy.getTable(customerSpace, migratedImportTableName);
        Long importCounts = getTableDataLines(migratedImportTable, yarnConfiguration);
        List<String> dataTables = dataFeedProxy.registerExtracts(customerSpace, getOutputDataFeedTaskId(customerSpace
                , convertServiceConfig), templateTable.getName(), migratedImportTable.getExtracts());
        updateConvertResult(customerSpace, convertServiceConfig, importCounts,
                dataTables);
    }

    @Override
    public Table verifyTenantStatus(String customerSpace,
                                   ConvertBatchStoreToImportServiceConfiguration convertServiceConfig) {
        String taskUniqueId = getOutputDataFeedTaskId(customerSpace, convertServiceConfig);
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
                                      Table masterTable, ConvertBatchStoreToImportServiceConfiguration config) {
        return templateTable.getAttributes().stream()
                .map(Attribute::getName)
                .distinct()
                .filter(attrName -> masterTable.getAttribute(attrName) == null)
                .collect(Collectors.toList());
    }

    @Override
    public Table getMasterTable(String customerSpace,
                                TableRoleInCollection batchStore,
                                ConvertBatchStoreToImportServiceConfiguration config) {
        return dataCollectionProxy.getTable(customerSpace, batchStore);
    }

    private ConvertBatchStoreInfo getConvertInfo(String customerSpace,
                                                 ConvertBatchStoreToImportServiceConfiguration config) {
        if (config == null || config.getConvertInfoPid() == null) {
            throw new RuntimeException("Cannot find ConvertInfo record with empty id!");
        }
        ConvertBatchStoreInfo convertBatchStoreInfo =
                convertBatchStoreInfoProxy.getConvertBatchStoreInfo(customerSpace, config.getConvertInfoPid());
        if (convertBatchStoreInfo == null || convertBatchStoreInfo.getConvertDetail() == null) {
            throw new RuntimeException("ConvertBatchStoreInfo record is not properly initialized!");
        }
        return convertBatchStoreInfo;
    }

}
