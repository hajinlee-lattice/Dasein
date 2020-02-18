package com.latticeengines.cdl.workflow.steps.importdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDynamoTableFromS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importDynamoTableFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDynamoTableFromS3 extends BaseImportExportS3<ImportDynamoTableFromS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(ImportDynamoTableFromS3.class);

    private String customerSpace;

    private Map<String, Table> tableMap = new HashMap<>();

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        buildImportedRequests(requests);
    }

    @SuppressWarnings("rawtypes")
    private void buildImportedRequests(List<ImportExportRequest> requests) {
        List<String> tableNames = configuration.getTableNames();
        if (CollectionUtils.isNotEmpty(tableNames)) {
            customerSpace = configuration.getCustomerSpace().toString();
            tableNames.forEach(tableName -> {
                Table table = metadataProxy.getTable(customerSpace, tableName);
                if (table != null) {
                    addTableToRequestForImport(table, requests);
                }
                tableMap.put(tableName, table);
            });
        } else {
            throw new RuntimeException("No table needs to migrate, just fail migrate workflow.");
        }
        if (CollectionUtils.isEmpty(requests)) {
            // means all data already on HDFS
            handleImportResult();
        }
    }

    private void updateDataUnit(DynamoDataUnit dynamoDataUnit) {
        log.info("Update signature to {} for dynamo data unit with name {} and tenant {}.",
                configuration.getDynamoSignature(), dynamoDataUnit.getName(), customerSpace);
        dataUnitProxy.updateSignature(customerSpace, dynamoDataUnit, configuration.getDynamoSignature());
    }

    @Override
    protected void handleImportResult() {
        List<DataUnit> dynamoDataUnits = dataUnitProxy.getByStorageType(customerSpace, DataUnit.StorageType.Dynamo);
        Map<String, DynamoDataUnit> dataUnitMap = dynamoDataUnits.stream()
                .collect(Collectors.toMap(DataUnit::getName, DataUnit -> (DynamoDataUnit) DataUnit));
        List<String> tableNames = configuration.getTableNames();
        for (String tableName : tableNames) {
            DynamoDataUnit dynamoDataUnit = dataUnitMap.get(tableName);
            if (dynamoDataUnit != null) {
                Table table = tableMap.get(tableName);
                if (table != null && CollectionUtils.isNotEmpty(table.getExtracts())) {
                    String hdfsPath = pathBuilder.getFullPath(table.getExtracts().get(0).getPath());
                    try {
                        if (HdfsUtils.fileExists(distCpConfiguration, hdfsPath)) {
                            exportToDynamo(dynamoDataUnit, hdfsPath);
                        } else {
                            // file still not in HDFS after import, so just update its signature
                            updateDataUnit(dynamoDataUnit);
                        }
                    } catch (IOException e) {
                        log.error("Can't get file info from HDFS with exception {}.", e.getMessage());
                    }
                } else {
                    updateDataUnit(dynamoDataUnit);
                }
            }
        }
    }

    private void exportToDynamo(DynamoDataUnit dynamoDataUnit, String hdfsPath) {
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(dynamoDataUnit.getName());
        config.setInputPath(hdfsPath);
        config.setPartitionKey(dynamoDataUnit.getPartitionKey());
        if (StringUtils.isNotBlank(dynamoDataUnit.getSortKey())) {
            config.setSortKey(dynamoDataUnit.getSortKey());
        }
        config.setLinkTableName(dynamoDataUnit.getLinkedTable());
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }
}
