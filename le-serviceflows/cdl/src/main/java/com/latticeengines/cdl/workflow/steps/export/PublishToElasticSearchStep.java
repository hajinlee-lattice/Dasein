package com.latticeengines.cdl.workflow.steps.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.PublishToElasticSearchStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component(PublishToElasticSearchStep.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class PublishToElasticSearchStep extends BaseImportExportS3<PublishToElasticSearchStepConfiguration> {

    static final String BEAN_NAME = "publishToElasticSearchStep";
    private static Logger log = LoggerFactory.getLogger(PublishToElasticSearchStep.class);

    private String customerSpace;
    private Map<TableRoleInCollection, List<Table>> tables;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        tables = new HashMap<>();
        if (CollectionUtils.isNotEmpty(configuration.getTableRoles())) {
            customerSpace = configuration.getCustomerSpace().toString();
            DataCollection.Version version = configuration.getVersion();
            configuration.getTableRoles().forEach(tableRole -> {
                List<Table> tableLists = dataCollectionProxy.getTables(customerSpace, tableRole, version);
                if (CollectionUtils.isNotEmpty(tableLists)) {
                    tableLists.forEach(table -> {
                        addTableToRequestForImport(table, requests);
                    });

                    tables.put(tableRole, tableLists);
                } else if (CollectionUtils.isEmpty(tableLists)) {
                    String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
                    log.info("There is no table for {} of version {} in tenant {}", tableRole, version, tenantId);
                }
            });
        } else {
            log.info("No tables to be published.");
        }
    }

    @Override
    protected void handleImportResult() {
        Map<TableRoleInCollection, List<HdfsDataUnit>> tableRoleListMap = new HashMap<>();
        tables.forEach((tableRole, tableLists) -> {
            List<HdfsDataUnit> tableUnits = new ArrayList<>();
            tableLists.forEach(table -> {
                String hdfsPath = pathBuilder.getFullPath(table.getExtracts().get(0).getPath());
                try {
                    if (HdfsUtils.fileExists(distCpConfiguration, hdfsPath)) {
                        String tableName = table.getName();
                        DataUnit oldDataUnit = dataUnitProxy.getByNameAndType(customerSpace, tableName, //
                                DataUnit.StorageType.ElasticSearch);
                        if (oldDataUnit != null) {
                            dataUnitProxy.delete(customerSpace, oldDataUnit);
                        }
                        tableUnits.add(table.toHdfsDataUnit(tableRole.name()));
                    } else {
                        // file still not on HDFS after import
                        throw new IllegalStateException("No file imported to hdfs");
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to import fless to hdfs", e);
                }
            });
            tableRoleListMap.put(tableRole, tableUnits);
        });
        putObjectInContext(TABLEROLES_GOING_TO_ES, tableRoleListMap);
    }
}
