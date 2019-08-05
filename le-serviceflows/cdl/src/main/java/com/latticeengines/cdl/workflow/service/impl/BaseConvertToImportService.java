package com.latticeengines.cdl.workflow.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreToImportServiceConfiguration;
import com.latticeengines.proxy.exposed.cdl.ConvertBatchStoreInfoProxy;

@Component("baseConvertToImportService")
@Lazy(value = false)
public class BaseConvertToImportService
        extends ConvertBatchStoreService<ConvertBatchStoreToImportServiceConfiguration> {

    @Inject
    private ConvertBatchStoreInfoProxy convertBatchStoreInfoProxy;

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
