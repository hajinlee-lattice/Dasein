package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateDynamoRequest;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.metadata.service.MetadataMigrateDynamoService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("metadataMigrateDynamoServiceImpl")
public class MetadataMigrateDynamoServiceImpl implements MetadataMigrateDynamoService {

    private static final Logger log = LoggerFactory.getLogger(MetadataMigrateDynamoServiceImpl.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Value("${common.quartz.stack.flag:false}")
    private boolean isQuartzStack;

    @Inject
    private DataUnitService dataUnitService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    private CDLProxy cdlProxy;

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Value("${common.adminconsole.url:}")
    private String quartzMicroserviceHostPort;

    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @Value("${metadata.dynamo.migrate.size}")
    private int migrateSize;

    private final int batchSize = 5;

    @PostConstruct
    public void initialize() {
        if (isQuartzStack) {
            cdlProxy = new CDLProxy(quartzMicroserviceHostPort);
        } else {
            cdlProxy = new CDLProxy(microserviceHostPort);
        }
    }

    private Tenant getTenant(Map<String, Tenant> tenantMap, String tenantId) {
        Tenant tenant = tenantMap.get(tenantId);
        if (tenant == null) {
            tenant = tenantEntityMgr.findByTenantId(tenantId);
            tenantMap.put(tenantId, tenant);
        }
        return tenant;
    }

    private String getTenantName(DynamoDataUnit dynamoDataUnit) {
        String tenantName = dynamoDataUnit.getLinkedTenant();
        if (StringUtils.isEmpty(tenantName)) {
            tenantName = dynamoDataUnit.getTenant();
        }
        return tenantName;
    }

    private String getTableName(DynamoDataUnit dynamoDataUnit) {
        String tableName = dynamoDataUnit.getLinkedTable();
        if (StringUtils.isEmpty(tableName)) {
            tableName = dynamoDataUnit.getName();
        }
        return tableName;
    }

    private List<String> getTableNames(Map<String, List<String>> tableMap, String tenantId) {
        List<String> tableNames = tableMap.get(tenantId);
        if (CollectionUtils.isEmpty(tableNames)) {
            tableNames = dataCollectionProxy.getTableNames(tenantId, null);
            tableMap.put(tenantId, tableNames);
        }
        return tableNames;
    }

    @Override
    public Boolean migrateDynamo() {
        List<DataUnit> dataUnits = dataUnitService.findByStorageType(DataUnit.StorageType.Dynamo);
        Map<String, Tenant> tenantMap = new HashMap<>();
        Map<String, List<String>> tableMap = new HashMap<>();
        List<DynamoDataUnit> dynamoDataUnits = new ArrayList<>();
        for (DataUnit dataUnit : dataUnits) {
            if (dynamoDataUnits.size() >= migrateSize) {
                log.info("Already found {} dynamo units in progress of migration.", migrateSize);
                break;
            }
            DynamoDataUnit dynamoDataUnit = (DynamoDataUnit) dataUnit;
            if (!signature.equals(dynamoDataUnit.getSignature())) {
                String tenantName = getTenantName(dynamoDataUnit);
                if (StringUtils.isNotEmpty(tenantName)) {
                    CustomerSpace customerSpace = CustomerSpace.parse(tenantName);
                    String tenantId = customerSpace.toString();
                    Tenant tenant = getTenant(tenantMap, tenantId);
                    // only few test dynamo records so we can migrate them with simple code
                    if (tenant != null && !tenant.getName().startsWith("LETest")) {
                        String tableName = getTableName(dynamoDataUnit);
                        if (StringUtils.isNotEmpty(tableName)) {
                            List<String> tableNames = getTableNames(tableMap, tenantId);
                            if (tableNames.contains(tableName)) {
                                if (!isTableInMigration(tableName)) {
                                    dynamoDataUnits.add(dynamoDataUnit);
                                    migrateTable(tenantId, tableName);
                                }
                            }
                        }
                    }
                }
            }
        }
        if (CollectionUtils.isEmpty(dynamoDataUnits)) {
            log.info("No dynamo data units need to migrate.");
        }
        return true;
    }

    private void migrateTable(String tenantId, String tableName) {
        MigrateDynamoRequest migrateDynamoRequest = new MigrateDynamoRequest();
        // if metadata collection table exists, metadata table should exist
        cdlProxy.submitMigrateDynamoJob(tenantId, migrateDynamoRequest);
    }

    // check if table is in progress of migration
    private boolean isTableInMigration(String tableName) {
        return false;
    }
}

