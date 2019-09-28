package com.latticeengines.cdl.workflow.service.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.RematchConvertServiceConfiguration;
import com.latticeengines.proxy.exposed.cdl.ConvertBatchStoreInfoProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("rematchConvertService")
@Lazy(value = false)
public class RematchConvertService extends ConvertBatchStoreService<RematchConvertServiceConfiguration> {

    @Inject
    private ConvertBatchStoreInfoProxy convertBatchStoreInfoProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Override
    public String getOutputDataFeedTaskId(String customerSpace, RematchConvertServiceConfiguration config) {
        return null;
    }

    @Override
    public Long getImportCounts(String customerSpace, RematchConvertServiceConfiguration config) {
        return null;
    }

    @Override
    public List<String> getRegisteredDataTables(String customerSpace, RematchConvertServiceConfiguration config) {
        return null;
    }

    @Override
    public Map<String, String> getDuplicateMap(String customerSpace, RematchConvertServiceConfiguration config) {
        return null;
    }

    @Override
    public Map<String, String> getRenameMap(String customerSpace, RematchConvertServiceConfiguration config) {
        return null;
    }

    @Override
    public void updateConvertResult(String customerSpace, RematchConvertServiceConfiguration config, Long importCounts, List<String> dataTables) {

    }

    @Override
    public void updateRegisteredAction(String customerSpace, RematchConvertServiceConfiguration config, Long actionId) {
        // do nothing.
    }

    @Override
    public void setDataTable(String migratedImportTableName, String customerSpace, Table templateTable, RematchConvertServiceConfiguration config, Configuration yarnConfiguration) {

    }

    @Override
    public Table verifyTenantStatus(String customerSpace, RematchConvertServiceConfiguration config) {
        return null;
    }

    @Override
    public List<String> getAttributes(String customerSpace, Table templateTable, RematchConvertServiceConfiguration config) {
        List<DataFeedTask> dataFeedTaskList =
                dataFeedProxy.getDataFeedTaskWithSameEntity(customerSpace,
                        config.getEntity().name());
        if (CollectionUtils.isEmpty(dataFeedTaskList)) {
            throw new RuntimeException(String.format("Cannot find the dataFeedTask in tenant %s, entity: %s.: " + customerSpace,
                    config.getEntity()));
        }
        Set<Attribute> attributeSet = new HashSet<>();
        for (DataFeedTask dataFeedTask : dataFeedTaskList) {
            templateTable = dataFeedTask.getImportTemplate();
            attributeSet.addAll(templateTable.getAttributes());
        }
        return attributeSet.stream().map(Attribute::getName).collect(Collectors.toList());
    }

    @Override
    public Table getMasterTable(String customerSpace,
                                TableRoleInCollection batchStore, RematchConvertServiceConfiguration config) {
        HashMap<TableRoleInCollection, Table> needConvertBatchStoreTables = config.getNeedConvertBatchStoreTables();
        if (needConvertBatchStoreTables == null) {
            return null;
        }
        return needConvertBatchStoreTables.get(batchStore);
    }
}
