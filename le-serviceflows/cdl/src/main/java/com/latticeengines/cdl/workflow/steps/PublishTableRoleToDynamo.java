package com.latticeengines.cdl.workflow.steps;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.PublishTableRoleStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("publishTableRoleToDynamo")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishTableRoleToDynamo extends BaseImportExportS3<PublishTableRoleStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PublishTableRoleToDynamo.class);

    private String customerSpace;
    private Map<TableRoleInCollection, Table> tables;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        tables = new HashMap<>();
        if (CollectionUtils.isNotEmpty(configuration.getTableRoles())) {
            customerSpace = configuration.getCustomerSpace().toString();
            DataCollection.Version version = configuration.getVersion();
            configuration.getTableRoles().forEach(tableRole -> {
                Table table = dataCollectionProxy.getTable(customerSpace, tableRole, version);
                if (table != null && tableRoleHasDynamoStore(tableRole)) {
                    addTableToRequestForImport(table, requests);
                    tables.put(tableRole, table);
                } else if (table == null) {
                    String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
                    log.info("There is no table for {} of version {} in tenant {}", tableRole, version, tenantId);
                } else {
                    log.info("Table role {} does not have dynamo store.", tableRole);
                }
            });
        } else {
            log.info("No tables to be published.");
        }
    }

    @Override
    protected void handleImportResult() {
        tables.forEach((tableRole, table) -> {
            String hdfsPath = pathBuilder.getFullPath(table.getExtracts().get(0).getPath());
            try {
                if (HdfsUtils.fileExists(distCpConfiguration, hdfsPath)) {
                    String tableName = table.getName();
                    DataUnit oldDataUnit = dataUnitProxy.getByNameAndType(customerSpace, tableName, //
                            DataUnit.StorageType.Dynamo);
                    if (oldDataUnit != null) {
                        dataUnitProxy.delete(customerSpace, oldDataUnit);
                    }
                    exportToDynamo(tableName, tableRole, hdfsPath);
                } else {
                    // file still not on HDFS after import, so just update its signature
                    throw new IllegalStateException("No file imported to hdfs");
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to import fless to hdfs", e);
            }
        });
    }

    private boolean tableRoleHasDynamoStore(TableRoleInCollection tableRole) {
        return tableRole.getPartitionKey() != null;
    }

    private void exportToDynamo(String tableName, TableRoleInCollection tableRole, String hdfsPath) {
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(PathUtils.toAvroGlob(hdfsPath));
        config.setPartitionKey(tableRole.getPartitionKey());
        if (StringUtils.isNotBlank(tableRole.getRangeKey())) {
            config.setSortKey(tableRole.getRangeKey());
        }
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }


}
